using Confluent.Kafka;

namespace Eventso.Subscription.Kafka.DeadLetter;

public interface IPoisonEventStore
{
    Task<long> CountPoisonedEvents(string groupId, string topic, CancellationToken token);

    Task<bool> IsKeyPoisoned(string groupId, string topic, ReadOnlyMemory<byte> key, CancellationToken token);

    IAsyncEnumerable<ReadOnlyMemory<byte>> GetPoisonedKeys(string groupId, TopicPartition topicPartition,
        CancellationToken token);

    Task AddEvent(string groupId, PoisonEvent @event, DateTime timestamp, string reason, CancellationToken token);

    Task RemoveEvent(string groupId, TopicPartitionOffset partitionOffset, CancellationToken token);

    // todo extract it from store interface to some scheduler
    Task<PoisonEvent?> GetEventForRetrying(string groupId, TopicPartition topicPartition, CancellationToken token);
}