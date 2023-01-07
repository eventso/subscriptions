using Eventso.Subscription.Kafka;

namespace Eventso.Subscription.Hosting;

public sealed class SubscriptionConfiguration
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

    public KafkaConsumerSettings Settings { get; }

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
}