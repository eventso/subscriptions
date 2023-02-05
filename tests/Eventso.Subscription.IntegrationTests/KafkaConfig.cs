using Confluent.Kafka;
using Eventso.Subscription.Kafka;

namespace Eventso.Subscription.IntegrationTests;

public sealed record KafkaConfig(
    string Brokers = "localhost:9092",
    string GroupId = null,
    string GroupInstanceId = "test-group-id")
{
    public ConsumerSettings ToSettings(
        string topic,
        PartitionAssignmentStrategy strategy = PartitionAssignmentStrategy.CooperativeSticky)
    {
        return new ConsumerSettings(
            Brokers,
            GroupId ?? Guid.NewGuid().ToString(), //slow rebalance for static group id
            groupInstanceId: GroupInstanceId)
        {
            Topic = topic,
            Config =
            {
                AutoCommitIntervalMs = 500,
                PartitionAssignmentStrategy = strategy
            }
        };
    }

    public KafkaConsumerSettings ToSettings(
        PartitionAssignmentStrategy strategy = PartitionAssignmentStrategy.CooperativeSticky)
    {
        return new KafkaConsumerSettings(
            Brokers,
            GroupId ?? Guid.NewGuid().ToString(), //slow rebalance for static group id
            groupInstanceId: GroupInstanceId)
        {
            Config =
            {
                AutoCommitIntervalMs = 500,
                PartitionAssignmentStrategy = strategy
            }
        };
    }
};