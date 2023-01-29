using System.Collections;
using System.Threading.Tasks.Dataflow;
using Eventso.Subscription.Configurations;

namespace Eventso.Subscription.Observing.Batch;

public sealed class BatchEventObserver<TEvent> : IObserver<TEvent>, IDisposable
    where TEvent : IEvent
{
    private readonly IEventHandler<TEvent> _handler;
    private readonly ActionBlock<Buffer<TEvent>.Batch> _actionBlock;
    private readonly CancellationTokenSource _cancellationTokenSource;
    private readonly IConsumer<TEvent> _consumer;
    private readonly IMessageHandlersRegistry _messageHandlersRegistry;
    private readonly bool _skipUnknown;
    private readonly Buffer<TEvent> _buffer;

    private bool _completed;
    private bool _disposed;

    public BatchEventObserver(
        BatchConfiguration config,
        IEventHandler<TEvent> handler,
        IConsumer<TEvent> consumer,
        IMessageHandlersRegistry messageHandlersRegistry,
        bool skipUnknown = true)
    {
        _handler = handler;
        _consumer = consumer ?? throw new ArgumentNullException(nameof(consumer));
        _messageHandlersRegistry = messageHandlersRegistry;
        _skipUnknown = skipUnknown;

        _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(
            consumer.CancellationToken);

        _actionBlock = new ActionBlock<Buffer<TEvent>.Batch>(
            async batch =>
            {
                using (batch.Events)
                    await HandleBatch(batch.Events, batch.ToBeHandledEventCount);
            },
            new ExecutionDataflowBlockOptions
            {
                CancellationToken = _cancellationTokenSource.Token,
                BoundedCapacity = 1
            });

        _buffer = new Buffer<TEvent>(
            config.MaxBatchSize,
            config.BatchTriggerTimeout,
            _actionBlock,
            config.GetMaxBufferSize(),
            _cancellationTokenSource.Token);
    }

    public Task OnEventAppeared(TEvent @event, CancellationToken token)
    {
        CheckDisposed();

        if (_completed)
            throw new InvalidOperationException("Batch observer is completed and can't accept more messages");

        if (_actionBlock.Completion.IsFaulted)
            return _actionBlock.Completion;

        _cancellationTokenSource.Token.ThrowIfCancellationRequested();

        var skipped = @event.CanSkip(_skipUnknown) ||
                      !_messageHandlersRegistry.ContainsHandlersFor(@event.GetMessage().GetType(), out _);

        return _buffer.Add(@event, skipped, token);
    }

    public async Task Complete(CancellationToken token)
    {
        CheckDisposed();

        if (_actionBlock.Completion.IsFaulted)
            await _actionBlock.Completion;

        _completed = true;
        await _buffer.Complete();

        _actionBlock.Complete();
        await _actionBlock.Completion;
    }

    public void Dispose()
    {
        _disposed = true;
        _buffer.Dispose();
        _actionBlock.Complete();

        if (!_cancellationTokenSource.IsCancellationRequested)
            _cancellationTokenSource.Cancel();

        _cancellationTokenSource.Dispose();
    }

    private async Task HandleBatch(PooledList<Buffer<TEvent>.BufferedEvent> events, int toBeHandledEventCount)
    {
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
        catch
        {
            _consumer.Cancel();
            throw;
        }
    }

    private static PooledList<TEvent> GetEventsToHandle(
        ReadOnlySpan<Buffer<TEvent>.BufferedEvent> messages, int handleMessageCount)
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