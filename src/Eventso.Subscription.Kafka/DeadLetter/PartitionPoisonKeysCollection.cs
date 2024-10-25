using System.Collections.Concurrent;
using Confluent.Kafka;

namespace Eventso.Subscription.Kafka.DeadLetter;

internal sealed class PartitionPoisonKeysCollection : IDisposable
{
    private readonly string _topic;
    private readonly Partition _partition;
    private readonly Task _loadTask;
    private readonly CancellationTokenSource _loadTaskCancellationTokenSource;
    private readonly ConcurrentDictionary<Guid, ValueTuple> _loadedKeys;

    public PartitionPoisonKeysCollection(
        Func<CancellationToken, Task> loadTask,
        string topic, 
        Partition partition)
    {
        _topic = topic;
        _partition = partition;
        _loadTaskCancellationTokenSource = new CancellationTokenSource();
        _loadTask = loadTask(_loadTaskCancellationTokenSource.Token);
        _loadedKeys = new ConcurrentDictionary<Guid, ValueTuple>();
    }

    public async Task WaitForReadiness(CancellationToken token)
        => await _loadTask.WaitAsync(token);

    public async Task TryAdd(Guid key, CancellationToken token)
    {
        await WaitForReadiness(token);
        _loadedKeys.TryAdd(key, default);
    }

    public void Reset(IReadOnlyCollection<Guid> keys)
    {
        _loadedKeys.Clear();

        foreach (var key in keys)
            _loadedKeys.TryAdd(key, default);
    }

    public async Task Remove(Guid key, CancellationToken token)
    {
        await WaitForReadiness(token);
        _loadedKeys.Remove(key, out _);
    }

    public bool ContainsKey(Guid key)
    {
        CheckReadiness();
        
        return _loadedKeys.ContainsKey(key);
    }

    public bool IsEmpty()
    {
        CheckReadiness();

        return _loadedKeys.IsEmpty;
    }

    private void CheckReadiness()
    {
        if (!_loadTask.IsCompleted)
            throw new Exception($"Partition #{_partition} in topic {_topic} is not ready yet");
    }

    public void Dispose()
    {
        _loadTaskCancellationTokenSource.Cancel();
        _loadTaskCancellationTokenSource.Dispose();

        if (_loadTask.IsFaulted)
            _ = _loadTask.Exception;
    }
}