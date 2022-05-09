using System.Collections.Generic;
using System.Threading;

namespace Eventso.Subscription.Http
{
    public sealed class Consumer : IConsumer<Event>
    {
        public Consumer(string topic) =>
            Subscription = topic;

        public CancellationToken CancellationToken => CancellationToken.None;
        
        public string Subscription { get; }

        public void Acknowledge(in Event events)
        {
        }

        public void Acknowledge(IReadOnlyList<Event> events)
        {
        }

        public void Cancel()
        {
        }
    }
}