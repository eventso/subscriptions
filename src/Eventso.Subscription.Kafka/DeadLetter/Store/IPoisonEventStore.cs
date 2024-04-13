using Confluent.Kafka;

namespace Eventso.Subscription.Kafka.DeadLetter.Store;

public interface IPoisonEventStore
{
    // poison event inbox
    
    Task<long> Count(string topic, CancellationToken token);

    Task<bool> IsStreamStored(string topic, Guid key, CancellationToken token);

    IAsyncEnumerable<StreamId> GetStoredStreams(IReadOnlyCollection<StreamId> streamIds, CancellationToken token);

    Task Add(DateTime timestamp, IReadOnlyCollection<OpeningPoisonEvent> events, CancellationToken token);

    Task Add(DateTime timestamp, OpeningPoisonEvent @event, CancellationToken token);
    
    // topic retrying service

    IAsyncEnumerable<StoredPoisonEvent> AcquireEventsForRetrying(string topic, CancellationToken token);

    // retrying event handler
    
    Task AddFailure(DateTime timestamp, OccuredFailure failure, CancellationToken token);

    Task AddFailures(DateTime timestamp, IReadOnlyCollection<OccuredFailure> failures, CancellationToken token);

    Task Remove(TopicPartitionOffset partitionOffset, CancellationToken token);

    Task Remove(IReadOnlyCollection<TopicPartitionOffset> partitionOffsets, CancellationToken token);
}

public interface IPoisonStreamCache
{
    ValueTask<bool> IsPoison(StreamId streamId, CancellationToken cancellationToken);

    ValueTask Add(StreamId streamId);

    ValueTask Remove(StreamId streamId);

    void Invalidate();
}

public interface IPoisonEventRetryingScheduler
{
    IAsyncEnumerable<StoredPoisonEvent> GetEventsForRetrying(string topic, CancellationToken token);
}