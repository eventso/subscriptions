using System.Collections.Concurrent;
using Confluent.Kafka;

namespace Eventso.Subscription.Kafka.DeadLetter;

public sealed class TopicPoisonKeysCollection(string topic)
{
    private readonly string _topic = topic;

    private readonly ConcurrentDictionary<Partition, PartitionKeys> _knownPartitions =
        new(concurrencyLevel: 1, capacity: 1);
    
    private long _version = 0;
    
    public void Register(Partition partition, Func<CancellationToken, Task<IReadOnlyCollection<Guid>>> poisonKeysTask)
    {
        var tokenSource = new CancellationTokenSource();
        _knownPartitions.TryAdd(
            partition,
            new PartitionKeys(CreateLoadTask(tokenSource.Token), tokenSource, new ConcurrentDictionary<Guid, ValueTuple>()));

        Interlocked.Increment(ref _version);

        async Task CreateLoadTask(CancellationToken token)
        {
            var keys = await poisonKeysTask(token);
            
            if (!_knownPartitions.TryGetValue(partition, out var partitionKeys))
                return;
            
            partitionKeys.LoadedKeys.Clear();
            foreach (var key in keys)
                partitionKeys.LoadedKeys.TryAdd(key, default);

            Interlocked.Increment(ref _version);
        }
    }

    public void Unregister(Partition partition)
    {
        if (!_knownPartitions.Remove(partition, out var partitionKeys))
            return;

        partitionKeys.LoadTaskCancellationTokenSource.Cancel();
        partitionKeys.LoadTaskCancellationTokenSource.Dispose();

        if (partitionKeys.LoadTask.IsFaulted)
            _ = partitionKeys.LoadTask.Exception;

        Interlocked.Increment(ref _version);
    }

    public async Task Add(Partition partition, Guid key, CancellationToken token)
    {
        if (!_knownPartitions.TryGetValue(partition, out var partitionKeys))
            throw new Exception($"Partition #{partition} in topic {_topic} is disabled");

        await partitionKeys.LoadTask.WaitAsync(token);
        partitionKeys.LoadedKeys.TryAdd(key, default);

        Interlocked.Increment(ref _version);
    }

    public async Task Remove(Partition partition, Guid key, CancellationToken token)
    {
        if (!_knownPartitions.TryGetValue(partition, out var partitionKeys))
            throw new Exception($"Partition #{partition} in topic {_topic} is disabled");

        await partitionKeys.LoadTask.WaitAsync(token);
        partitionKeys.LoadedKeys.Remove(key, out _);

        Interlocked.Increment(ref _version);
    }

    public async Task<KeySet> GetKeys(CancellationToken token)
    {
        foreach (var (_, partitionKeys) in _knownPartitions)
            await partitionKeys.LoadTask.WaitAsync(token);

        return new KeySet(this, Interlocked.Read(ref _version));
    }

    private sealed record PartitionKeys(
        Task LoadTask,
        CancellationTokenSource LoadTaskCancellationTokenSource,
        ConcurrentDictionary<Guid, ValueTuple> LoadedKeys);
    
    public sealed record KeySet(TopicPoisonKeysCollection TopicKeysCollection, long OnCreationVersion) : IKeySet<Event>
    {
        public ICollection<Partition> GetPartitions()
        {
            CheckVersion();

            return TopicKeysCollection._knownPartitions.Keys;
        }
        
        public bool Contains(in Event item)
        {
            CheckVersion();

            return TopicKeysCollection._knownPartitions.TryGetValue(item.Partition, out var partitionKeys)
                ? partitionKeys.LoadedKeys.ContainsKey(item.GetKey())
                : throw new Exception($"Partition #{item.Partition} in topic {TopicKeysCollection._topic} is disabled");
        }

        public bool IsEmpty()
        {
            CheckVersion();

            foreach (var (_, partitionKeys) in TopicKeysCollection._knownPartitions)
            {
                if (partitionKeys.LoadedKeys.Count > 0)
                    return false;
            }

            return true;
        }

        private void CheckVersion()
        {
            var currentVersion = Interlocked.Read(ref TopicKeysCollection._version);
            if (currentVersion != OnCreationVersion)
                throw new Exception($"Key set is outdated for topic {TopicKeysCollection._topic}.");
        }
    }
}