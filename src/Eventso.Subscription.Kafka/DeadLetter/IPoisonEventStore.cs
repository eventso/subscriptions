using Confluent.Kafka;

namespace Eventso.Subscription.Kafka.DeadLetter;

public interface IPoisonEventStore
{
    Task<long> CountPoisonedEvents(string groupId, string topic, CancellationToken token);

    Task<bool> IsKeyPoisoned(string groupId, string topic, byte[] key, CancellationToken token);

    IAsyncEnumerable<byte[]> GetPoisonedKeys(
        string groupId,
        TopicPartition topicPartition,
        CancellationToken token);

    IAsyncEnumerable<TopicPartitionOffset> GetPoisonedOffsets(
        string groupId,
        string topic,
        CancellationToken token);

    Task<PoisonEvent> GetEvent(string groupId, TopicPartitionOffset partitionOffset, CancellationToken token);

    Task AddEvent(string groupId, ConsumeResult<byte[], byte[]> @event, DateTime timestamp, string reason, CancellationToken token);

    Task RemoveEvent(string groupId, TopicPartitionOffset partitionOffset, CancellationToken token);
}