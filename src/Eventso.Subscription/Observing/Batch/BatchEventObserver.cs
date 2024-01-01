using System.Collections;
using System.Diagnostics;
using System.Threading.Channels;
using Eventso.Subscription.Configurations;

namespace Eventso.Subscription.Observing.Batch;

public sealed class BatchEventObserver<TEvent> : IObserver<TEvent>, IDisposable
    where TEvent : IEvent, IGroupedMetadata<TEvent>
{
    private readonly IEventHandler<TEvent> _handler;
    private readonly Channel<Buffer<TEvent>.Batch> _batchChannel;
    private readonly CancellationTokenSource _cancellationTokenSource;
    private readonly IConsumer<TEvent> _consumer;
    private readonly IMessageHandlersRegistry _messageHandlersRegistry;
    private readonly ILogger<BatchEventObserver<TEvent>> _logger;
    private readonly bool _skipUnknown;
    private readonly Buffer<TEvent> _buffer;

    private bool _completed;
    private bool _disposed;
    private readonly Task _batchHandlingTask;

    public BatchEventObserver(
        BatchConfiguration config,
        IEventHandler<TEvent> handler,
        IConsumer<TEvent> consumer,
        IMessageHandlersRegistry messageHandlersRegistry,
        ILogger<BatchEventObserver<TEvent>> logger,
        bool skipUnknown = true)
    {
        _handler = handler;
        _consumer = consumer ?? throw new ArgumentNullException(nameof(consumer));
        _messageHandlersRegistry = messageHandlersRegistry;
        _logger = logger;
        _skipUnknown = skipUnknown;

        _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(
            consumer.CancellationToken);

        _batchChannel = Channel.CreateBounded<Buffer<TEvent>.Batch>(
            new BoundedChannelOptions(capacity: 1)
            {
                SingleReader = true,
                SingleWriter = false,
                FullMode = BoundedChannelFullMode.Wait
            });

        _batchHandlingTask = Task.Run(BeginBatchHandling);

        _buffer = new Buffer<TEvent>(
            config.MaxBatchSize,
            config.BatchTriggerTimeout,
            _batchChannel,
            config.GetMaxBufferSize(),
            _cancellationTokenSource.Token);
    }

    public Task OnEventAppeared(TEvent @event, CancellationToken token)
    {
        CheckDisposed();

        if (_batchHandlingTask.IsFaulted)
            return _batchHandlingTask;

        if (_batchChannel.Reader.Completion.IsFaulted)
            return _batchChannel.Reader.Completion;

        if (_completed || _batchChannel.Reader.Completion.IsCompleted || _batchHandlingTask.IsCompleted)
            throw new InvalidOperationException("Batch observer is completed and can't accept more messages");

        _cancellationTokenSource.Token.ThrowIfCancellationRequested();

        var skipped = @event.CanSkip(_skipUnknown) ||
            !_messageHandlersRegistry.ContainsHandlersFor(@event.GetMessage().GetType(), out _);

        return _buffer.Add(@event, skipped, token);
    }

    public async Task Complete(CancellationToken token)
    {
        CheckDisposed();

        if (_batchChannel.Reader.Completion.IsFaulted)
            await _batchChannel.Reader.Completion;

        _completed = true;
        await _buffer.Complete();

        _batchChannel.Writer.TryComplete();
        await _batchChannel.Reader.Completion;
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        _buffer.Dispose();
        _batchChannel.Writer.TryComplete();

        _cancellationTokenSource.Cancel();
        _cancellationTokenSource.Dispose();

        // drain potentially unobserved task exception
        // it's observed elsewhere via _batchChannel's Writer.TryComplete(ex) and Reader.Completion
        if (_batchHandlingTask.IsFaulted)
            _ = _batchHandlingTask.Exception;
    }

    private async Task BeginBatchHandling()
    {
        try
        {
            while (await _batchChannel.Reader.WaitToReadAsync(_cancellationTokenSource.Token))
            {
                while (_batchChannel.Reader.TryPeek(out var batch))
                {
                    using (batch.Events)
                        await HandleBatch(batch.Events, batch.ToBeHandledEventCount);

                    _batchChannel.Reader.TryRead(out _);
                }
            }
        }
        catch (Exception ex)
        {
            _batchChannel.Writer.TryComplete(ex);

            //cleanup queue
            while (_batchChannel.Reader.TryRead(out _)) ;

            throw;
        }
    }

    private async Task HandleBatch(PooledList<Buffer<TEvent>.BufferedEvent> events, int toBeHandledEventCount)
    {
        using var tracingScope = CreateBatchActivity(events);

        if (events.Count == 0)
            return;

        var allEvents = new EventsCollection(events);

        try
        {
            if (events.Count == toBeHandledEventCount)
            {
                await _handler.Handle(allEvents, _cancellationTokenSource.Token);
            }
            else if (toBeHandledEventCount > 0)
            {
                using var eventsToHandle = GetEventsToHandle(events.Span, toBeHandledEventCount);

                await _handler.Handle(eventsToHandle, _cancellationTokenSource.Token);
            }

            _consumer.Acknowledge(allEvents);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Handling batch failed.");

            _consumer.Cancel();
            tracingScope.Activity?.SetException(ex);

            throw;
        }
    }

    private static Diagnostic.RootActivityScope CreateBatchActivity(PooledList<Buffer<TEvent>.BufferedEvent> events)
    {
        var scope = Diagnostic.StartRooted("batch.handle");
        if (scope.Activity is { } activity)
        {
            var ts = DateTimeOffset.UtcNow;
            foreach (var metadataGroup in TEvent.GroupedMetadata(events.Select(static e => e.Event)))
            {
                activity.AddEvent(new ActivityEvent(
                    name: "batch.metadata",
                    timestamp: ts,
                    tags: new ActivityTagsCollection(metadataGroup!)));
            }
        }

        return scope;
    }

    private static PooledList<TEvent> GetEventsToHandle(
        ReadOnlySpan<Buffer<TEvent>.BufferedEvent> messages,
        int handleMessageCount)
    {
        var list = new PooledList<TEvent>(handleMessageCount);

        foreach (ref readonly var message in messages)
        {
            if (!message.Skipped)
                list.Add(message.Event);
        }

        return list;
    }

    private void CheckDisposed()
    {
        if (_disposed) throw new ObjectDisposedException("Batch observer is disposed");
    }

    private sealed class EventsCollection : IConvertibleCollection<TEvent>
    {
        private readonly PooledList<Buffer<TEvent>.BufferedEvent> _messages;

        public EventsCollection(PooledList<Buffer<TEvent>.BufferedEvent> messages)
            => _messages = messages;

        public int Count => _messages.Count;

        public TEvent this[int index]
            => _messages[index].Event;

        public IReadOnlyCollection<TOut> Convert<TOut>(Converter<TEvent, TOut> converter)
            => _messages.Convert(i => converter(i.Event));

        public bool OnlyContainsSame<TValue>(Func<TEvent, TValue> valueConverter)
        {
            if (Count == 0)
                return true;

            var comparer = EqualityComparer<TValue>.Default;
            var sample = valueConverter(_messages[0].Event);

            foreach (ref readonly var item in _messages.Span)
            {
                if (!comparer.Equals(valueConverter(item.Event), sample))
                    return false;
            }

            return true;
        }

        public IEnumerator<TEvent> GetEnumerator()
        {
            foreach (var item in _messages.Segment)
                yield return item.Event;
        }

        IEnumerator IEnumerable.GetEnumerator()
            => GetEnumerator();
    }
}
