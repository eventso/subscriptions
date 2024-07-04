namespace Eventso.Subscription.Observing;

public sealed class EventObserver<TEvent> : IObserver<TEvent>
    where TEvent : IEvent
{
    private readonly IEventHandler<TEvent> _eventHandler;
    private readonly IConsumer<TEvent> _consumer;
    private readonly IMessageHandlersRegistry _messageHandlersRegistry;

    private readonly bool _skipUnknown;

    public EventObserver(
        IEventHandler<TEvent> eventHandler,
        IConsumer<TEvent> consumer,
        IMessageHandlersRegistry messageHandlersRegistry,
        bool skipUnknown)
    {
        _eventHandler = eventHandler;
        _consumer = consumer;
        _messageHandlersRegistry = messageHandlersRegistry;
        _skipUnknown = skipUnknown;
    }

    public async Task OnEventAppeared(TEvent @event, CancellationToken token)
    {
        if (@event.CanSkip(_skipUnknown))
        {
            _consumer.Acknowledge(@event);
            return;
        }

        var hasHandler = _messageHandlersRegistry.ContainsHandlersFor(
            @event.GetMessage().GetType(), out var handlerKind);

        if (!hasHandler)
        {
            _consumer.Acknowledge(@event);
            return;
        }

        if ((handlerKind & HandlerKind.Single) == 0)
            throw new InvalidOperationException(
                $"There is no single message handler for subscription {_consumer.Subscription}");

        await _eventHandler.Handle(@event, token);

        _consumer.Acknowledge(@event);
    }


    public void Dispose()
    {
    }
}