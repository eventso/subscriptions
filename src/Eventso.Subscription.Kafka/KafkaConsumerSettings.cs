using Confluent.Kafka;

namespace Eventso.Subscription.Kafka;

public class KafkaConsumerSettings
{
    private readonly Func<ConsumerConfig, ConsumerBuilder<Guid, ConsumedMessage>> _builderFactory =
        c => new ConsumerBuilder<Guid, ConsumedMessage>(c);

    public KafkaConsumerSettings()
    {
        Config = new ConsumerConfig
        {
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
        };
    }

    public KafkaConsumerSettings(
        string brokers,
        string groupId,
        TimeSpan? maxPollInterval = default,
        TimeSpan? sessionTimeout = default,
        AutoOffsetReset autoOffsetReset = AutoOffsetReset.Earliest,
        string? groupInstanceId = null)
        : this(groupId, maxPollInterval, sessionTimeout, autoOffsetReset, groupInstanceId)
    {
        Config.BootstrapServers = brokers;
    }

    public KafkaConsumerSettings(
        Func<ConsumerConfig, ConsumerBuilder<Guid, ConsumedMessage>> builderFactory,
        string groupId,
        TimeSpan? maxPollInterval = default,
        TimeSpan? sessionTimeout = default,
        AutoOffsetReset autoOffsetReset = AutoOffsetReset.Earliest,
        string? groupInstanceId = null)
        : this(groupId, maxPollInterval, sessionTimeout, autoOffsetReset, groupInstanceId)
    {
        _builderFactory = builderFactory;
    }

    private KafkaConsumerSettings(
        string groupId,
        TimeSpan? maxPollInterval = default,
        TimeSpan? sessionTimeout = default,
        AutoOffsetReset autoOffsetReset = AutoOffsetReset.Earliest,
        string? groupInstanceId = null)
        : this()
    {
        Config.GroupId = groupId;
        Config.AutoOffsetReset = autoOffsetReset;

        if (groupInstanceId != null)
            Config.GroupInstanceId = groupInstanceId;

        if (maxPollInterval.HasValue)
            Config.MaxPollIntervalMs = (int)maxPollInterval.Value.TotalMilliseconds;

        if (sessionTimeout.HasValue)
            Config.SessionTimeoutMs = (int)sessionTimeout.Value.TotalMilliseconds;
    }

    public ConsumerConfig Config { get; }

    public ConsumerBuilder<Guid, ConsumedMessage> CreateBuilder()
        => _builderFactory(Config);
}