namespace Eventso.Subscription.Observing.DeadLetter;

public interface IPoisonEventInbox<in TEvent>
    where TEvent : IEvent
{
    ValueTask<bool> IsPartOfPoisonStream(TEvent @event, CancellationToken token);
    Task Add(TEvent @event, string reason, CancellationToken token);
}