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

        IAsyncEnumerable<Guid> GetStoredKeys(
            string topic,
            IReadOnlyCollection<Guid> key,
            CancellationToken cancellationToken);

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

    // TODO remove (created just for successful build)
    public sealed record StoredFailure(DateTime arg1, string arg2);
    public sealed record StoredEvent(TopicPartitionOffset arg1, Guid arg2, byte[] arg3, DateTime arg4, StoredEventHeader[] arg5);
    public sealed record StoredPoisonEvent;
    public sealed record StoredEventHeader(string arg1, byte[] arg2);
}