using Confluent.Kafka;

namespace Eventso.Subscription.Kafka;

public sealed class NullConsumerEventsObserver : IConsumerEventsObserver
{
    public static NullConsumerEventsObserver Instance = new();

    public void OnAssign(IReadOnlyCollection<TopicPartition> topicPartitions)
    {
    }

    public void OnRevoke(IReadOnlyCollection<TopicPartitionOffset> topicPartitions)
    {
    }

    public void OnClose(IReadOnlyCollection<TopicPartition> topicPartitions)
    {
    }

    public Task<bool> TryHandleSerializationException(ConsumeException exception, CancellationToken token)
        => Task.FromResult(false);
}