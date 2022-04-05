using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Eventso.Subscription.Kafka.DeadLetter.Store
{
    public interface IPoisonEventStore
    {
        IAsyncEnumerable<StoredPoisonEvent> GetEventsForRetrying(string topic, CancellationToken cancellationToken);

        Task<long> Count(string topic, CancellationToken cancellationToken);

        Task<bool> Any(string topic, Guid key, CancellationToken cancellationToken);

        Task Add(
            StoredEvent @event,
            StoredFailure failure,
            CancellationToken cancellationToken);

        Task AddFailure(
            TopicPartitionOffset topicPartitionOffset,
            StoredFailure failure,
            CancellationToken cancellationToken);

        Task Remove(TopicPartitionOffset topicPartitionOffset, CancellationToken cancellationToken);
    }
}