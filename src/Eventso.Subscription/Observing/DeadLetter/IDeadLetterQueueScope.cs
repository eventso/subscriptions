using System;
using System.Collections.Generic;

namespace Eventso.Subscription.Observing.DeadLetter
{
    public interface IDeadLetterQueueScope<TEvent> : IDisposable
        where TEvent : IEvent
    {
        IReadOnlyCollection<PoisonEvent<TEvent>> GetPoisonEvents();
    }
}