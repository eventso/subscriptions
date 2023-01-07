using Confluent.Kafka;

namespace Eventso.Subscription.Kafka;

public sealed class ConsumerSettings : KafkaConsumerSettings
{
    public ConsumerSettings() : base()
    {
    }

    public ConsumerSettings(
        string brokers,
        string groupId,
        TimeSpan? maxPollInterval = default,
        TimeSpan? sessionTimeout = default,
        AutoOffsetReset autoOffsetReset = AutoOffsetReset.Earliest,
        PartitionAssignmentStrategy assignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky)
        : base(brokers, groupId, maxPollInterval, sessionTimeout, autoOffsetReset, assignmentStrategy)
    {
    }

    public string Topic { get; set; }
}