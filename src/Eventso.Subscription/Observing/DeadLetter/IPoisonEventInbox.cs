namespace Eventso.Subscription.Observing.DeadLetter;

public interface IPoisonEventInbox<TEvent> where TEvent : IEvent
{
    Task<IKeySet<TEvent>> GetEventKeys(string topic, CancellationToken token);  
    Task Add(TEvent @event, string reason, CancellationToken token);
}
