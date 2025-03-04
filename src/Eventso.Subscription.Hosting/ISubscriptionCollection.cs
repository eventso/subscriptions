using Eventso.Subscription.Kafka;

namespace Eventso.Subscription.Hosting;

public interface ISubscriptionCollection : IEnumerable<SubscriptionConfiguration>
{
    ISubscriptionCollection Add(
        ConsumerSettings settings,
        IMessageDeserializer serializer,
        HandlerConfiguration? handlerConfig = default,
        bool skipUnknownMessages = true,
        int instances = 1,
        TimeSpan? messageObservingDelay = default);

    ISubscriptionCollection AddBatch(
        ConsumerSettings settings,
        BatchConfiguration batchConfig,
        IMessageDeserializer serializer,
        HandlerConfiguration? handlerConfig = default,
        bool skipUnknownMessages = true,
        int instances = 1,
        TimeSpan? messageObservingDelay = default);

    ISubscriptionCollection AddMultiTopic(
        KafkaConsumerSettings settings,
        Action<IMultiTopicSubscriptionCollection> subscriptions,
        int instances = 1);

    ISubscriptionCollection Add(SubscriptionConfiguration configuration);
}