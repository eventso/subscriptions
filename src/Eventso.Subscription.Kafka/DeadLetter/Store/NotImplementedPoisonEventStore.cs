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
        
        public IAsyncEnumerable<StoredPoisonEvent> GetEventsForRetrying(string topic, CancellationToken cancellationToken)
            => throw new NotImplementedException(ErrorMessage);

        public Task<long> Count(string topic, CancellationToken cancellationToken)
            => throw new NotImplementedException(ErrorMessage);

        public Task<bool> Any(string topic, Guid key, CancellationToken cancellationToken)
            => throw new NotImplementedException(ErrorMessage);

        public Task Add(StoredEvent @event, StoredFailure failure, CancellationToken cancellationToken)
            => throw new NotImplementedException(ErrorMessage);

        public Task AddFailure(
            TopicPartitionOffset topicPartitionOffset,
            StoredFailure failure,
            CancellationToken cancellationToken)
            => throw new NotImplementedException(ErrorMessage);

        public Task Remove(TopicPartitionOffset topicPartitionOffset, CancellationToken cancellationToken)
            => throw new NotImplementedException(ErrorMessage);
    }
}