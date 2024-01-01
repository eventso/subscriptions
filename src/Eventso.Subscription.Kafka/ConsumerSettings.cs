using Confluent.Kafka;

namespace Eventso.Subscription.Kafka;

public sealed record ConsumerSettings : KafkaConsumerSettings
{
    public ConsumerSettings()
    {
    }

    public ConsumerSettings(
        string brokers,
        string groupId,
        string? groupInstanceId,
        TimeSpan? maxPollInterval = default,
        AutoOffsetReset autoOffsetReset = AutoOffsetReset.Earliest)
        : base(brokers, groupId, groupInstanceId, maxPollInterval, autoOffsetReset)
    {
    }

    /// <summary>
    /// Note: Auto-generated group.instance.id
    /// </summary>
    public ConsumerSettings(
        string brokers,
        string groupId,
        TimeSpan? maxPollInterval = default,
        AutoOffsetReset autoOffsetReset = AutoOffsetReset.Earliest)
        : base(brokers, groupId, maxPollInterval, autoOffsetReset)
    {
    }

    public ConsumerSettings(
        Func<ConsumerConfig, ConsumerBuilder<Guid, ConsumedMessage>> builderFactory,
        string groupId,
        string? groupInstanceId,
        TimeSpan? maxPollInterval = default,
        AutoOffsetReset autoOffsetReset = AutoOffsetReset.Earliest)
        : base(builderFactory, groupId, groupInstanceId, maxPollInterval, autoOffsetReset)
    {
    }

    /// <summary>
    /// Note: Auto-generated group.instance.id
    /// </summary>
    public ConsumerSettings(
        Func<ConsumerConfig, ConsumerBuilder<Guid, ConsumedMessage>> builderFactory,
        string groupId,
        TimeSpan? maxPollInterval = default,
        AutoOffsetReset autoOffsetReset = AutoOffsetReset.Earliest)
        : base(builderFactory, groupId, maxPollInterval, autoOffsetReset)
    {
    }


    public string Topic { get; set; } = null!;
}