using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Eventso.Subscription.Kafka.DeadLetter.Store
{
    public sealed class NotImplementedPoisonEventStore : IPoisonEventStore
    {
        private static readonly string ErrorMessage =
            $"Implementation of {nameof(IPoisonEventStore)} was not provided.";
        
        public IAsyncEnumerable<StoredPoisonEvent> AcquireEventsForRetrying(string topic, CancellationToken cancellationToken)
            => throw new NotImplementedException(ErrorMessage);

        public Task<long> Count(string topic, CancellationToken cancellationToken)
            => throw new NotImplementedException(ErrorMessage);

        public Task<bool> IsStreamStored(string topic, Guid key, CancellationToken token)
            => throw new NotImplementedException(ErrorMessage);

        public IAsyncEnumerable<StreamId> GetStoredStreams(IReadOnlyCollection<StreamId> streamIds, CancellationToken token)
            => throw new NotImplementedException(ErrorMessage);

        public Task Add(DateTime timestamp, IReadOnlyCollection<OpeningPoisonEvent> events, CancellationToken token)
            => throw new NotImplementedException(ErrorMessage);

        public Task Add(DateTime timestamp, OpeningPoisonEvent @event, CancellationToken token)
            => throw new NotImplementedException(ErrorMessage);

        public Task AddFailure(DateTime timestamp, OccuredFailure failure, CancellationToken token)
            => throw new NotImplementedException(ErrorMessage);

        public Task AddFailures(DateTime timestamp, IReadOnlyCollection<OccuredFailure> failures, CancellationToken token)
            => throw new NotImplementedException(ErrorMessage);

        public Task Remove(TopicPartitionOffset partitionOffset, CancellationToken token)
            => throw new NotImplementedException(ErrorMessage);

        public Task Remove(IReadOnlyCollection<TopicPartitionOffset> topicPartitionOffsets, CancellationToken token)
            => throw new NotImplementedException(ErrorMessage);
    }
}