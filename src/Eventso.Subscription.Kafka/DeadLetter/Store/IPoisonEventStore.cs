using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Eventso.Subscription.Kafka.DeadLetter.Store
{
    public interface IPoisonEventStore
    {
        Task<long> Count(string topic, CancellationToken token);

        IAsyncEnumerable<StoredPoisonEvent> GetEventsForRetrying(string topic, CancellationToken token);

        Task<bool> IsStreamStored(string topic, Guid key, CancellationToken token);

        IAsyncEnumerable<StreamId> GetStoredStreams(
            IReadOnlyCollection<StreamId> streamIds,
            CancellationToken token);

        Task Add(
            DateTime timestamp,
            IReadOnlyCollection<OpeningPoisonEvent> events,
            CancellationToken token);

        Task AddFailures(
            DateTime timestamp,
            IReadOnlyCollection<OccuredFailure> failures,
            CancellationToken token);

        Task Remove(
            IReadOnlyCollection<TopicPartitionOffset> partitionOffsets,
            CancellationToken token);
    }
    
    // TODO remove (created just for successful build)
    public sealed record OccuredFailure(TopicPartitionOffset arg1, string arg2);
    public sealed record OpeningPoisonEvent(TopicPartitionOffset arg1, Guid arg2, ReadOnlyMemory<byte> arg3, DateTime arg4, IReadOnlyCollection<EventHeader> arg5, string arg6);
    public sealed record StoredPoisonEvent(
        TopicPartitionOffset TopicPartitionOffset,
        Guid Key,
        ReadOnlyMemory<byte> Value,
        DateTime CreationTimestamp,
        IReadOnlyCollection<EventHeader> Headers);
    public sealed record StreamId(string arg1, Guid arg2); // will be struct with IEquatable
    public sealed record EventHeader(string Key, byte[] Data);
}