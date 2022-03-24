using System.Collections.Generic;
using System.Threading;

namespace Eventso.Subscription.InMemory
{
    public sealed class Consumer : IConsumer<Message>
    {
        public Consumer(string topic) =>
            Subscription = topic;

        public CancellationToken CancellationToken => CancellationToken.None;
        
        public string Subscription { get; }

        public void Acknowledge(in Message message)
        {
        }

        public void Acknowledge(IReadOnlyList<Message> messages)
        {
        }

        public void Cancel()
        {
        }
    }
}