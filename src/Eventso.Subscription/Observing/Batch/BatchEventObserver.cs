using System.Threading.Channels;
using Eventso.Subscription.Configurations;

namespace Eventso.Subscription.Observing.Batch;

public sealed class BatchEventObserver<TEvent> : IObserver<TEvent>
    where TEvent : IEvent
{
    private readonly Channel<Buffer<TEvent>.Batch> _batchChannel;
    private readonly CancellationTokenSource _cancellationTokenSource;
    private readonly BatchHandler<TEvent> _handler;
    private readonly IMessageHandlersRegistry _messageHandlersRegistry;
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
        _handler = new BatchHandler<TEvent>(handler, consumer, logger);
        _messageHandlersRegistry = messageHandlersRegistry;
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
                        await _handler.HandleBatch(batch.Events, batch.ToBeHandledEventCount, _cancellationTokenSource.Token);

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

    private void CheckDisposed()
    {
        if (_disposed) throw new ObjectDisposedException("Batch observer is disposed");
    }
}