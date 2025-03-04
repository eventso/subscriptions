using System.Collections.Immutable;
using Confluent.Kafka;
using Eventso.Subscription.Kafka.DeadLetter;

namespace Eventso.Subscription.Hosting.DeadLetter;

internal sealed class DevNullPoisonEventStore : IPoisonEventStore, IPoisonEventRetryScheduler
{
    public static DevNullPoisonEventStore Instance { get; } = new DevNullPoisonEventStore();

    public Task<IReadOnlyDictionary<ConsumingTarget, long>> CountPoisonedEvents(CancellationToken token)
        => Task.FromResult((IReadOnlyDictionary<ConsumingTarget, long>)ImmutableDictionary<ConsumingTarget, long>.Empty);

    public Task<long> CountPoisonedEvents(string groupId, string topic, CancellationToken token)
        => Task.FromResult(0L);

    public Task<bool> IsKeyPoisoned(string groupId, string topic, byte[] key, CancellationToken token)
        => Task.FromResult(false);

    public IAsyncEnumerable<byte[]> GetPoisonedKeys(string groupId, TopicPartition topicPartition, CancellationToken token)
        => AsyncEnumerable.Empty<byte[]>();

    public IAsyncEnumerable<TopicPartitionOffset> GetPoisonedOffsets(string groupId, string topic, CancellationToken token)
        => AsyncEnumerable.Empty<TopicPartitionOffset>();

    public Task<PoisonEvent> GetEvent(string groupId, TopicPartitionOffset partitionOffset, CancellationToken token)
        => throw new NotImplementedException();

    public Task AddEvent(
        string groupId,
        ConsumeResult<byte[], byte[]> @event,
        DateTime timestamp, string reason,
        CancellationToken token)
        => Task.CompletedTask;

    public Task RemoveEvent(string groupId, TopicPartitionOffset partitionOffset, CancellationToken token)
        => Task.CompletedTask;

    public Task<ConsumeResult<byte[], byte[]>?> GetNextRetryTarget(string groupId, TopicPartition topicPartition, CancellationToken token)
        => Task.FromResult<ConsumeResult<byte[], byte[]>?>(default);
}