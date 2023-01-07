using Eventso.Subscription.Kafka;

namespace Eventso.Subscription.Hosting;

public interface ISubscriptionCollection : IEnumerable<SubscriptionConfiguration>
{
    ISubscriptionCollection Add(
        ConsumerSettings settings,
        IMessageDeserializer serializer,
        HandlerConfiguration handlerConfig = default,
        DeferredAckConfiguration deferredAckConfig = default,
        bool skipUnknownMessages = true,
        int instances = 1);

    ISubscriptionCollection AddBatch(
        ConsumerSettings settings,
        BatchConfiguration batchConfig,
        IMessageDeserializer serializer,
        HandlerConfiguration handlerConfig = default,
        bool skipUnknownMessages = true,
        int instances = 1);

    ISubscriptionCollection AddMultiTopic(
        KafkaConsumerSettings settings,
        Action<IMultiTopicSubscriptionCollection> subscriptions,
        int bufferSize = 0,
        int instances = 1);

    ISubscriptionCollection Add(SubscriptionConfiguration configuration);
}