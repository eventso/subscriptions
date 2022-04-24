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
}