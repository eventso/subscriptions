namespace Eventso.Subscription.Hosting;

public interface IMultiTopicSubscriptionCollection
{
    IMultiTopicSubscriptionCollection Add(
        string topic,
        IMessageDeserializer serializer,
        HandlerConfiguration? handlerConfig = default,
        DeferredAckConfiguration? deferredAckConfig = default,
        bool skipUnknownMessages = true,
        int bufferSize = 10,
        TimeSpan? messageObservingDelay = default,
        bool enableDeadLetterQueue = false);

    IMultiTopicSubscriptionCollection AddBatch(
        string topic,
        BatchConfiguration batchConfig,
        IMessageDeserializer serializer,
        HandlerConfiguration? handlerConfig = default,
        bool skipUnknownMessages = true,
        int bufferSize = 0,
        TimeSpan? messageObservingDelay = default,
        bool enableDeadLetterQueue = false);

    IMultiTopicSubscriptionCollection Add(TopicSubscriptionConfiguration configuration);
}