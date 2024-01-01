using Eventso.Subscription.Kafka;

namespace Eventso.Subscription.Hosting;

public sealed record SubscriptionConfiguration
{
    public SubscriptionConfiguration(
        KafkaConsumerSettings settings,
        int consumerInstances,
        params TopicSubscriptionConfiguration[] topicConfigurations)
    {
        if (consumerInstances < 1)
            throw new ArgumentOutOfRangeException(
                nameof(consumerInstances),
                consumerInstances,
                "Instances should be >= 1.");

        if (topicConfigurations is null || topicConfigurations.Length == 0)
            throw new ArgumentException("Value cannot be null or empty collection.", nameof(topicConfigurations));

        Settings = settings ?? throw new ArgumentNullException(nameof(settings));
        ConsumerInstances = consumerInstances;
        TopicConfigurations = topicConfigurations;
    }

    public SubscriptionConfiguration(
        KafkaConsumerSettings settings,
        params TopicSubscriptionConfiguration[] topics)
        : this(settings, consumerInstances: 1, topics)
    {
    }

    public int ConsumerInstances { get; }

    public TopicSubscriptionConfiguration[] TopicConfigurations { get; }

    public KafkaConsumerSettings Settings { get; private init; }

    public TopicSubscriptionConfiguration GetByTopic(string topic)
    {
        foreach (var configuration in TopicConfigurations)
        {
            if (configuration.Topic.Equals(topic))
                return configuration;
        }

        throw new InvalidOperationException($"Configuration for topic '{topic}' not found.");
    }

    public string[] GetTopics()
        => TopicConfigurations.Select(t => t.Topic).ToArray();

    public bool Contains(string topic)
        => TopicConfigurations.Any(c => c.Topic.Equals(topic));

    internal IEnumerable<SubscriptionConfiguration> ClonePerConsumerInstance()
    {
        if (ConsumerInstances == 1)
            yield return this;
        else
        {
            foreach (var i in  Enumerable.Range(0, ConsumerInstances))
            {
                yield return this with { Settings = Settings.GetForInstance(i) };
            }
        }
    }
}