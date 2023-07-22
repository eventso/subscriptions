using Confluent.Kafka;

namespace Eventso.Subscription.Kafka;

public sealed class ConsumerSettings : KafkaConsumerSettings
{
    public ConsumerSettings()
    {
    }

    public ConsumerSettings(
        string brokers,
        string groupId,
        TimeSpan? maxPollInterval = default,
        TimeSpan? sessionTimeout = default,
        AutoOffsetReset autoOffsetReset = AutoOffsetReset.Earliest,
        string? groupInstanceId = null)
        : base(brokers, groupId, maxPollInterval, sessionTimeout, autoOffsetReset, groupInstanceId)
    {
    }

    public ConsumerSettings(
        Func<ConsumerConfig, ConsumerBuilder<Guid, ConsumedMessage>> builderFactory,
        string groupId,
        TimeSpan? maxPollInterval = default,
        TimeSpan? sessionTimeout = default,
        AutoOffsetReset autoOffsetReset = AutoOffsetReset.Earliest,
        string? groupInstanceId = null)
        : base(builderFactory, groupId, maxPollInterval, sessionTimeout, autoOffsetReset, groupInstanceId)
    {
    }


    public string Topic { get; set; } = null!;
}