using System;
using System.Collections.Generic;

namespace Eventso.Subscription.InMemory.Hosting
{
    public interface ISubscriptionCollection : IEnumerable<SubscriptionConfiguration>
    {
        ISubscriptionCollection Add(string topic, IMessageDeserializer deserializer);

        ISubscriptionCollection AddBatch(
            string topic,
            IMessageDeserializer deserializer,
            TimeSpan? batchTriggerTimeout = null);
    }
}