namespace Eventso.Subscription.Observing.DeadLetter
{
    public interface IPoisonStreamCollection<in TEvent>
        where TEvent : IEvent
    {
        bool IsPartOfPoisonStream(TEvent @event);
    }
}