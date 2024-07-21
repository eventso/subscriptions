namespace Eventso.Subscription;

public interface IEventHandler<TEvent>
    where TEvent : IEvent
{
    Task Handle(TEvent @event, HandlingContext context, CancellationToken token);

    Task Handle(IConvertibleCollection<TEvent> events, HandlingContext context, CancellationToken token);
}