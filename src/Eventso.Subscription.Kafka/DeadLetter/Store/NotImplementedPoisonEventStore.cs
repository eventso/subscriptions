using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Eventso.Subscription.Kafka.DeadLetter.Store
{
    public sealed class NotImplementedPoisonEventStore : IPoisonEventStore
    {
        private static readonly string ErrorMessage =
            $"Implementation of {nameof(IPoisonEventStore)} was not provided.";
        
        public IAsyncEnumerable<StoredPoisonEvent> GetEventsForRetrying(string topic, CancellationToken cancellationToken)
            => throw new NotImplementedException(ErrorMessage);

        public Task<long> Count(string topic, CancellationToken cancellationToken)
            => throw new NotImplementedException(ErrorMessage);

        public Task<bool> IsKeyStored(string topic, Guid key, CancellationToken cancellationToken)
            => throw new NotImplementedException(ErrorMessage);

        public IAsyncEnumerable<Guid> GetStoredKeys(string topic, IReadOnlyCollection<Guid> keys, CancellationToken cancellationToken)
            => throw new NotImplementedException(ErrorMessage);

        public Task Add(string topic, DateTime timestamp, IReadOnlyCollection<OpeningPoisonEvent> events, CancellationToken token)
            => throw new NotImplementedException(ErrorMessage);

        public Task AddFailures(string topic, DateTime timestamp, IReadOnlyCollection<OccuredFailure> failures, CancellationToken token)
            => throw new NotImplementedException(ErrorMessage);

        public Task Remove(string topic, IReadOnlyCollection<PartitionOffset> partitionOffsets, CancellationToken token)
            => throw new NotImplementedException(ErrorMessage);
    }
}