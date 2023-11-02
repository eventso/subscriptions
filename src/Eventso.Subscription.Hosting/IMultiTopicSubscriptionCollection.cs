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
        TimeSpan? messageObservingDelay = default);

    IMultiTopicSubscriptionCollection AddBatch(
        string topic,
        BatchConfiguration batchConfig,
        IMessageDeserializer serializer,
        HandlerConfiguration? handlerConfig = default,
        bool skipUnknownMessages = true,
        TimeSpan? messageObservingDelay = default);

    IMultiTopicSubscriptionCollection Add(TopicSubscriptionConfiguration configuration);
}