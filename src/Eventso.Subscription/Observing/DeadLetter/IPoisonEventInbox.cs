namespace Eventso.Subscription.Observing.DeadLetter;

public interface IPoisonEventInbox<TEvent>
    where TEvent : IEvent
{
    Task Add(PoisonEvent<TEvent> @event, CancellationToken cancellationToken);
    Task Add(IReadOnlyCollection<PoisonEvent<TEvent>> events, CancellationToken cancellationToken);

    Task<bool> IsPartOfPoisonStream(TEvent @event, CancellationToken cancellationToken);

    Task<IPoisonStreamCollection<TEvent>?> GetPoisonStreams(
        IReadOnlyCollection<TEvent> events,
        CancellationToken cancellationToken);
}