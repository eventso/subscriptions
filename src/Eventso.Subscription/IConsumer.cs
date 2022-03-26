using System.Collections.Generic;
using System.Threading;

namespace Eventso.Subscription
{
    public interface IConsumer<TEvent>
        where TEvent : IEvent
    {
        CancellationToken CancellationToken { get; }
        string Subscription { get; }

        void Acknowledge(in TEvent events);
        void Acknowledge(IReadOnlyList<TEvent> events);

        void Cancel();
    }
}