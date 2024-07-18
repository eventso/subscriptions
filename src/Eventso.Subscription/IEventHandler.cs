namespace Eventso.Subscription;

public interface IEventHandler<in TEvent>
    where TEvent : IEvent
{
    Task Handle(TEvent @event, HandlingContext context, CancellationToken cancellationToken);

    Task Handle(IConvertibleCollection<TEvent> events, HandlingContext context, CancellationToken cancellationToken);
}