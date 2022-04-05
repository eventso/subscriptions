using System.Collections.Generic;

namespace Eventso.Subscription.Observing.DeadLetter
{
    public interface IDeadLetterQueueScopeFactory
    {
        IDeadLetterQueueScope<TEvent> Create<TEvent>(string topic, TEvent @event)
            where TEvent : IEvent;

        IDeadLetterQueueScope<TEvent> Create<TEvent>(string topic, IReadOnlyCollection<TEvent> events)
            where TEvent : IEvent;
    }
}