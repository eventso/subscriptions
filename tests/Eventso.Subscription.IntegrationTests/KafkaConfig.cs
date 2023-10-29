using Eventso.Subscription.Kafka;

namespace Eventso.Subscription.IntegrationTests;

public sealed record KafkaConfig(
    string Brokers = "localhost:9092",
    string? GroupId = null,
    string GroupInstanceId = "test-group-id")
{
    public ConsumerSettings ToSettings(
        string? topic,
        bool enableAutoCommit = false,
        TimeSpan? pauseAfter = default)
    {
        return new ConsumerSettings(
            Brokers,
            GroupId ?? Guid.NewGuid().ToString(), //slow rebalance for static group id
            groupInstanceId: GroupInstanceId)
        {
            Topic = topic!,
            Config =
            {
                AutoCommitIntervalMs = 500,
                EnableAutoCommit = enableAutoCommit
            },
            PauseAfterObserveDelay = pauseAfter
        };
    }

    public KafkaConsumerSettings ToSettings(
        bool enableAutoCommit = false,
        TimeSpan? pauseAfter = default)
        => ToSettings(null, enableAutoCommit, pauseAfter);
};