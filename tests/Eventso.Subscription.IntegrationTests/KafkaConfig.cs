using Eventso.Subscription.Kafka;

namespace Eventso.Subscription.IntegrationTests;

public sealed record KafkaConfig(
    string Brokers = "localhost:9092",
    string GroupId = null,
    string GroupInstanceId = "test-group-id")
{
    public static implicit operator KafkaConsumerSettings(KafkaConfig config)
    {
        return new KafkaConsumerSettings(
            config.Brokers,
            config.GroupId ?? Guid.NewGuid().ToString(), //slow rebalance for static group id
            groupInstanceId: config.GroupInstanceId);
    }
};