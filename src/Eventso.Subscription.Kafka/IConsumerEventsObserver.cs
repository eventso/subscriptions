using Confluent.Kafka;

namespace Eventso.Subscription.Kafka;

public interface IConsumerEventsObserver
{
    void OnAssign(IReadOnlyCollection<TopicPartition> topicPartitions);

    void OnRevoke(IReadOnlyCollection<TopicPartitionOffset> topicPartitions);

    void OnClose(IReadOnlyCollection<TopicPartition> topicPartitions);

    Task<bool> TryHandleSerializationException(ConsumeException exception, CancellationToken token);
}