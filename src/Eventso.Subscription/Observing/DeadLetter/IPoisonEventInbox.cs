namespace Eventso.Subscription.Observing.DeadLetter;

public interface IPoisonEventInbox<TEvent> where TEvent : IEvent
{
    ValueTask<IKeySet<TEvent>> GetEventKeys(string topic);  
    Task Add(TEvent @event, string reason, CancellationToken token);
}
