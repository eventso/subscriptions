using Eventso.Subscription.Hosting;
using Eventso.Subscription.SpanJson;

namespace Eventso.Subscription.IntegrationTests;

public static class Extensions
{
    public static IMultiTopicSubscriptionCollection AddJson<T>(
        this IMultiTopicSubscriptionCollection collection,
        string topic,
        int bufferSize = 10)
        => collection.Add(topic, new JsonMessageDeserializer<T>(), bufferSize: bufferSize);

    public static IMultiTopicSubscriptionCollection AddBatchJson<T>(
        this IMultiTopicSubscriptionCollection collection,
        string topic,
        int batchSize = 20,
        TimeSpan? batchTimeout = default)
    {
        return collection.AddBatch(topic,
            new BatchConfiguration
            {
                BatchTriggerTimeout = batchTimeout ?? TimeSpan.FromSeconds(0.5),
                MaxBatchSize = batchSize
            },
            new JsonMessageDeserializer<T>());
    }
}