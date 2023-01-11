using Eventso.Subscription.Kafka;

namespace Eventso.Subscription.IntegrationTests;

public sealed record KafkaConfig(
    string Brokers = "localhost:9092",
    string GroupId = "test-group",
    string GroupInstanceId = "test-group-id")
{
    public static implicit operator KafkaConsumerSettings(KafkaConfig config)
    {
        return new KafkaConsumerSettings(
            config.Brokers,
            Guid.NewGuid().ToString(), //config.GroupId,
            groupInstanceId: config.GroupInstanceId);
    }
};