using Eventso.Subscription.Configurations;

namespace Eventso.Subscription.Observing;

public sealed class EventObserver<TEvent> : IObserver<TEvent>, IDisposable
    where TEvent : IEvent
{
    private readonly IEventHandler<TEvent> _eventHandler;
    private readonly IConsumer<TEvent> _consumer;
    private readonly IMessageHandlersRegistry _messageHandlersRegistry;

    private readonly bool _skipUnknown;
    private readonly DeferredAckConfiguration _deferredAckConfig;
    private readonly ILogger<EventObserver<TEvent>> _logger;
    private readonly List<TEvent> _skipped = new();
    private readonly Timer _deferredAckTimer;

    public EventObserver(
        IEventHandler<TEvent> eventHandler,
        IConsumer<TEvent> consumer,
        IMessageHandlersRegistry messageHandlersRegistry,
        bool skipUnknown,
        DeferredAckConfiguration deferredAckConfiguration,
        ILogger<EventObserver<TEvent>> logger)
    {
        _eventHandler = eventHandler;
        _consumer = consumer;
        _messageHandlersRegistry = messageHandlersRegistry;
        _skipUnknown = skipUnknown;
        _deferredAckConfig = deferredAckConfiguration;
        _logger = logger;
        _deferredAckTimer = new Timer(_ => AckDeferredMessages(isTimeout: true));
    }

    public async Task OnEventAppeared(TEvent @event, CancellationToken token)
    {
        if (@event.CanSkip(_skipUnknown))
        {
            DeferAck(@event);
            return;
        }

        var hasHandler = _messageHandlersRegistry.ContainsHandlersFor(
            @event.GetMessage().GetType(), out var handlerKind);

        if (!hasHandler)
        {
            DeferAck(@event);
            return;
        }

        await Task.Yield();

        var metadata = @event.GetMetadata();

        using var scope = metadata.Count > 0 ? _logger.BeginScope(metadata) : null;

        AckDeferredMessages();

        if ((handlerKind & HandlerKind.Single) == 0)
            throw new InvalidOperationException(
                $"There is no single message handler for subscription {_consumer.Subscription}");

        await _eventHandler.Handle(@event, token);

        _consumer.Acknowledge(@event);
    }

    public void Dispose()
        => _deferredAckTimer.Dispose();

    private void DeferAck(in TEvent skippedMessage)
    {
        if (_deferredAckConfig.MaxBufferSize <= 1)
        {
            _consumer.Acknowledge(skippedMessage);
            return;
        }

        lock (_skipped)
            _skipped.Add(skippedMessage);

        if (_skipped.Count >= _deferredAckConfig.MaxBufferSize)
            AckDeferredMessages();
        else
            _deferredAckTimer.Change(_deferredAckConfig.Timeout, Timeout.InfiniteTimeSpan);
    }

    private void AckDeferredMessages(bool isTimeout = false)
    {
        if (_skipped.Count == 0)
            return;

        if (!isTimeout)
            _deferredAckTimer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);

        lock (_skipped)
        {
            _consumer.Acknowledge(_skipped);
            _skipped.Clear();
        }
    }
}