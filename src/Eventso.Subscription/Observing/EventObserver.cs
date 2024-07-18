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
        await Handle(@event, token);

        _consumer.Acknowledge(@event);
    }

    private Task Handle(TEvent @event, CancellationToken token)
    {
        if (@event.CanSkip(_skipUnknown))
            return Task.CompletedTask;

        var hasHandler = _messageHandlersRegistry.ContainsHandlersFor(
            @event.GetMessage().GetType(), out var handlerKind);

        if (!hasHandler)
            return Task.CompletedTask;

        if ((handlerKind & HandlerKind.Single) == 0)
            throw new InvalidOperationException(
                $"There is no single message handler for subscription {_consumer.Subscription}");

        return _eventHandler.Handle(@event, new HandlingContext(), token);
    }

    public void Dispose()
    {
    }
}