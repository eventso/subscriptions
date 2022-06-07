using System.Collections.Generic;
using Eventso.Subscription.Configurations;
using Eventso.Subscription.Kafka;

namespace Eventso.Subscription.Hosting
{
    public interface ISubscriptionCollection : IEnumerable<SubscriptionConfiguration>
    {
        ISubscriptionCollection Add(
            ConsumerSettings settings,
            IMessageDeserializer serializer,
            HandlerConfiguration handlerConfig = default,
            DeferredAckConfiguration deferredAckConfig = default,
            bool skipUnknownMessages = true,
            int instances = 1,
            bool enableDeadLetterQueue = false);

        ISubscriptionCollection AddBatch(
            ConsumerSettings settings,
            BatchConfiguration batchConfig,
            IMessageDeserializer serializer,
            HandlerConfiguration handlerConfig = default,
            bool skipUnknownMessages = true,
            int instances = 1,
            bool enableDeadLetterQueue = false);

        ISubscriptionCollection Add(SubscriptionConfiguration configuration);
    }
}