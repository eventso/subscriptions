using Confluent.Kafka;

namespace Eventso.Subscription.Kafka.DeadLetter.Store;

public interface IPoisonEventStore
{
    Task<long> Count(string topic, CancellationToken token);

    IAsyncEnumerable<StoredPoisonEvent> AcquireEventsForRetrying(string topic, CancellationToken token);

    Task<bool> IsStreamStored(string topic, Guid key, CancellationToken token);

    IAsyncEnumerable<StreamId> GetStoredStreams(IReadOnlyCollection<StreamId> streamIds, CancellationToken token);

    Task Add(DateTime timestamp, IReadOnlyCollection<OpeningPoisonEvent> events, CancellationToken token);

    Task Add(DateTime timestamp, OpeningPoisonEvent @event, CancellationToken token);

    Task AddFailure(DateTime timestamp, OccuredFailure failure, CancellationToken token);

    Task AddFailures(DateTime timestamp, IReadOnlyCollection<OccuredFailure> failures, CancellationToken token);

    Task Remove(TopicPartitionOffset partitionOffset, CancellationToken token);

    Task Remove(IReadOnlyCollection<TopicPartitionOffset> partitionOffsets, CancellationToken token);
}