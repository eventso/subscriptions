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

    /// <summary>
    /// Note: Auto-generated group.instance.id
    /// </summary>
    public KafkaConsumerSettings(
        string brokers,
        string groupId,
        TimeSpan? maxPollInterval = default,
        AutoOffsetReset autoOffsetReset = AutoOffsetReset.Earliest)
        : this(groupId, maxPollInterval, autoOffsetReset, Guid.NewGuid().ToString())
    {
        Config.BootstrapServers = brokers;
    }

    public KafkaConsumerSettings(
        Func<ConsumerConfig, ConsumerBuilder<Guid, ConsumedMessage>> builderFactory,
        string groupId,
        string? groupInstanceId,
        TimeSpan? maxPollInterval = default,
        AutoOffsetReset autoOffsetReset = AutoOffsetReset.Earliest)
        : this(groupId, maxPollInterval, autoOffsetReset, groupInstanceId)
    {
        _builderFactory = builderFactory;
    }

    public KafkaConsumerSettings(
        string brokers,
        string groupId,
        string? groupInstanceId,
        TimeSpan? maxPollInterval = default,
        AutoOffsetReset autoOffsetReset = AutoOffsetReset.Earliest)
        : this(groupId, maxPollInterval, autoOffsetReset, groupInstanceId)
    {
        Config.BootstrapServers = brokers;
    }


    /// <summary>
    /// Note: Auto-generated group.instance.id
    /// </summary>
    public KafkaConsumerSettings(
        Func<ConsumerConfig, ConsumerBuilder<Guid, ConsumedMessage>> builderFactory,
        string groupId,
        TimeSpan? maxPollInterval = default,
        AutoOffsetReset autoOffsetReset = AutoOffsetReset.Earliest)
        : this(groupId, maxPollInterval, autoOffsetReset, Guid.NewGuid().ToString())
    {
        _builderFactory = builderFactory;
    }

    private KafkaConsumerSettings(
        string groupId,
        TimeSpan? maxPollInterval,
        AutoOffsetReset autoOffsetReset,
        string? groupInstanceId)
        : this()
    {
        Config.GroupId = groupId;
        Config.AutoOffsetReset = autoOffsetReset;

        if (groupInstanceId != null)
            Config.GroupInstanceId = groupInstanceId;

        if (maxPollInterval.HasValue)
            Config.MaxPollIntervalMs = (int)maxPollInterval.Value.TotalMilliseconds;
    }

    private KafkaConsumerSettings(
        ConsumerConfig config,
        Func<ConsumerConfig, ConsumerBuilder<Guid, ConsumedMessage>> builderFactory)
    {
        Config = config;
        _builderFactory = builderFactory;
    }

    public ConsumerConfig Config { get; }

    /// <summary>
    /// Topic will be paused when observe takes longer then the value. Default: session.timeout.ms
    /// </summary>
    public TimeSpan? PauseAfterObserveDelay { get; init; }

    public ConsumerBuilder<Guid, ConsumedMessage> CreateBuilder()
        => _builderFactory(Config);

    public KafkaConsumerSettings GetForInstance(int consumerInstanceNumber)
    {
        if (Config.GroupInstanceId == null || consumerInstanceNumber == 0)
            return this;

        var config = new ConsumerConfig(new Dictionary<string, string>(Config));

        config.GroupInstanceId += "#" + consumerInstanceNumber;

        return new KafkaConsumerSettings(
            config,
            _builderFactory);
    }
}