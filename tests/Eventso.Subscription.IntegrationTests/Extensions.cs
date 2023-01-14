using Eventso.Subscription.Hosting;
using Eventso.Subscription.SpanJson;

namespace Eventso.Subscription.IntegrationTests;

public static class Extensions
{
    public static IMultiTopicSubscriptionCollection AddJson<T>(
        this IMultiTopicSubscriptionCollection collection,
        string topic,
        int bufferSize = 10)
    {
        return collection.Add(topic, new JsonMessageDeserializer<T>(),
            bufferSize: bufferSize);
    }

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

    public static async Task<TestHost> RunHost(this IServiceCollection serviceCollection)
    {
        var host = new TestHost(serviceCollection);
        try
        {
            await host.Start();
        }
        catch
        {
            await host.DisposeAsync();
            throw;
        }

        return host;
    }

    public static TestHost CreateHost(this IServiceCollection serviceCollection)
        => new TestHost(serviceCollection);

    public static async Task WhenAll(this TestHost host, params Task[] tasks)
    {
        var waiting = Task.WhenAll(tasks);

        await await Task.WhenAny(waiting, host.FailedCompletion);
    }

    public record TopicMessages<T>(string Topic, T[] Messages);

    public record ColoredTopics(
        TopicMessages<RedMessage> Red,
        TopicMessages<GreenMessage> Green,
        TopicMessages<BlueMessage> Blue,
        TopicMessages<BlackMessage> Black);


    public static async Task<ColoredTopics> CreateTopics(
        this TopicSource topicSource,
        IFixture fixture,
        int messageCount = 100)
    {
        var topicRed = await topicSource.CreateTopicWithMessages<RedMessage>(fixture, messageCount);
        var topicGreen = await topicSource.CreateTopicWithMessages<GreenMessage>(fixture, messageCount);
        var topicBlue = await topicSource.CreateTopicWithMessages<BlueMessage>(fixture, messageCount);
        var topicBlack = await topicSource.CreateTopicWithMessages<BlackMessage>(fixture, messageCount);

        return new(
            new(topicRed.topic, topicRed.messages),
            new(topicGreen.topic, topicGreen.messages),
            new(topicBlue.topic, topicBlue.messages),
            new(topicBlack.topic, topicBlack.messages));
    }

    public static IEnumerable<T> GetByIndex<T>(this T[] items, params Index[] indexes)
        => indexes.Select(index => items[index]);
}