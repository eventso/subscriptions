using System.Collections.Concurrent;
using Confluent.Kafka;

namespace Eventso.Subscription.Kafka.DeadLetter;

public sealed class TopicPoisonKeysCollection(string topic)
{
    private readonly string _topic = topic;

    private readonly ConcurrentDictionary<Partition, PartitionPoisonKeysCollection> _knownPartitions =
        new(concurrencyLevel: 1, capacity: 1);
    
    public void Register(Partition partition, Func<CancellationToken, Task<IReadOnlyCollection<Guid>>> poisonKeysTask)
    {
        _knownPartitions.TryAdd(
            partition, 
            new PartitionPoisonKeysCollection(
                CreateLoadTask,
                _topic,
                partition));

        async Task CreateLoadTask(CancellationToken token)
        {
            var keys = await poisonKeysTask(token);
            
            if (!_knownPartitions.TryGetValue(partition, out var partitionKeys))
                return;

            partitionKeys.Reset(keys);
        }
    }

    public void Unregister(Partition partition)
    {
        if (!_knownPartitions.Remove(partition, out var partitionKeys))
            return;

        partitionKeys.Dispose();
    }

    public async Task Add(Partition partition, Guid key, CancellationToken token)
    {
        if (!_knownPartitions.TryGetValue(partition, out var partitionKeys))
            throw new Exception($"Partition #{partition} in topic {_topic} is disabled");

        await partitionKeys.TryAdd(key, token);
    }

    public async Task Remove(Partition partition, Guid key, CancellationToken token)
    {
        if (!_knownPartitions.TryGetValue(partition, out var partitionKeys))
            throw new Exception($"Partition #{partition} in topic {_topic} is disabled");

        await partitionKeys.Remove(key, token);
    }

    public async Task<KeySet> GetKeys(CancellationToken token)
    {
        foreach (var (_, partitionKeys) in _knownPartitions)
            await partitionKeys.WaitForReadiness(token);

        return new KeySet(this);
    }

    public sealed record KeySet(TopicPoisonKeysCollection TopicKeysCollection) : IKeySet<Event>
    {
        public ICollection<Partition> GetPartitions()
            => TopicKeysCollection._knownPartitions.Keys;

        public bool Contains(in Event item)
        {
            return TopicKeysCollection._knownPartitions.TryGetValue(item.Partition, out var partitionKeys)
                ? partitionKeys.ContainsKey(item.GetKey())
                : throw new Exception($"Partition #{item.Partition} in topic {TopicKeysCollection._topic} is disabled");
        }

        public bool IsEmpty()
        {
            foreach (var (_, partitionKeys) in TopicKeysCollection._knownPartitions)
            {
                if (!partitionKeys.IsEmpty())
                    return false;
            }
            
            return true;
        }
    }
}