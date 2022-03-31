using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Eventso.Subscription.Kafka.DeadLetter.Store
{
    public interface IPoisonEventStore
    {
        IAsyncEnumerable<StoredPoisonEvent> GetEventsForRetrying(string topic, CancellationToken token);

        Task<long> Count(string topic, CancellationToken token);

        Task<bool> IsKeyStored(string topic, Guid key, CancellationToken token);

        IAsyncEnumerable<Guid> GetStoredKeys(
            string topic,
            IReadOnlyCollection<Guid> keys,
            CancellationToken token);

        Task Add(
            string topic,
            DateTime timestamp,
            IReadOnlyCollection<OpeningPoisonEvent> poisonEvents,
            CancellationToken token);

        Task AddFailures(
            string topic,
            DateTime timestamp,
            IReadOnlyCollection<RecentFailure> failures,
            CancellationToken token);

        Task Remove(
            string topic,
            IReadOnlyCollection<PartitionOffset> partitionOffsets,
            CancellationToken token);
    }
}