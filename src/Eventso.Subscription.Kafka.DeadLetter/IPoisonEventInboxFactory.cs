namespace Eventso.Subscription.Kafka.DeadLetter;

public interface IPoisonEventInboxFactory<TEvent>
    where TEvent : IEvent
{
    public IPoisonEventInbox<TEvent> Create(string topic);
}