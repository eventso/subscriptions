using Eventso.Subscription.Observing.DeadLetter;

namespace Eventso.Subscription.Hosting;

public interface IPoisonEventInboxFactory<TEvent>
    where TEvent : IEvent
{
    public IPoisonEventInbox<TEvent> Create(string topic);
}