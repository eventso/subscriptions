namespace Eventso.Subscription.Observing.DeadLetter;

public interface IDeadLetterQueueScopeFactory
{
    IDeadLetterQueueScope<TEvent> Create<TEvent>(TEvent @event)
        where TEvent : IEvent;

    IDeadLetterQueueScope<TEvent> Create<TEvent>(IReadOnlyCollection<TEvent> events)
        where TEvent : IEvent;
}