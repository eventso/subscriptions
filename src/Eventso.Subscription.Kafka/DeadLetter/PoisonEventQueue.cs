using System.Runtime.CompilerServices;
using Confluent.Kafka;

namespace Eventso.Subscription.Kafka.DeadLetter;

public sealed class PoisonEventQueue(
    IPoisonEventStore poisonEventStore,
    IPoisonEventRetryingScheduler retryingScheduler,
    string groupId,
    int maxNumberOfPoisonedEventsInTopic)
    : IPoisonEventQueue
{
    private readonly Dictionary<TopicPartition, Task<HashSet<Guid>>> _partitionPoisonKeys = new();

    public bool IsEnabled => true;

    public void Assign(TopicPartition topicPartition)
    {
        // no concurrency, same thread as consume
        _partitionPoisonKeys[topicPartition] = GetPoisonKeys();

        async Task<HashSet<Guid>> GetPoisonKeys()
        {
            var poisonedKeys = new HashSet<Guid>();

            var keysSource = poisonEventStore.GetPoisonedKeys(groupId, topicPartition, CancellationToken.None);
            await foreach (var key in keysSource)
                poisonedKeys.Add(DeserializeKey(key.Span));
            return poisonedKeys;
        }
    }

    public void Revoke(TopicPartition topicPartition)
    {
        // no concurrency, same thread as consume
        _partitionPoisonKeys.Remove(topicPartition);
    }

    public async ValueTask<bool> IsPoison(TopicPartition topicPartition, Guid key, CancellationToken token)
    {
        var keys = await GetTopicPartitionKeys(topicPartition, token);
        return keys.Contains(key);
    }

    public async Task Blame(PoisonEvent @event, DateTime failureTimestamp, string failureReason, CancellationToken token)
    {
        var alreadyPoisoned = await poisonEventStore.CountPoisonedEvents(groupId, @event.TopicPartitionOffset.Topic, token);
        if (alreadyPoisoned >= maxNumberOfPoisonedEventsInTopic)
            throw new EventHandlingException(
                @event.TopicPartitionOffset.Topic,
                $"Dead letter queue exceeds {maxNumberOfPoisonedEventsInTopic} size.",
                null);
        
        await poisonEventStore.AddEvent(groupId, @event, failureTimestamp, failureReason, token);
        
        var keys = await GetTopicPartitionKeys(@event.TopicPartitionOffset.TopicPartition, token);
        keys.Add(DeserializeKey(@event.Key.Span));
    }

    public async IAsyncEnumerable<PoisonEvent> GetEventsForRetrying([EnumeratorCancellation] CancellationToken token)
    {
        bool needToProcess = true;

        while (needToProcess)
        {
            needToProcess = false;
            var topicPartitions = _partitionPoisonKeys.Keys.ToArray();
            foreach (var topicPartition in topicPartitions)
            {
                try
                {
                    var keys = await GetTopicPartitionKeys(topicPartition, token);
                    if (keys.Count == 0)
                        continue;
                }
                catch (Exception)
                {
                    continue;
                }

                var eventForRetrying = await retryingScheduler.GetEventForRetrying(groupId, topicPartition, token);
                if (eventForRetrying == null)
                    continue;

                needToProcess = true;
                yield return eventForRetrying;
            }
        }
    }

    private static Guid DeserializeKey(ReadOnlySpan<byte> keySpan)
        => KeyGuidDeserializer.Instance.Deserialize(keySpan, false, SerializationContext.Empty);

    public async Task Rehabilitate(PoisonEvent @event, CancellationToken token)
    {
        await poisonEventStore.RemoveEvent(groupId, @event.TopicPartitionOffset, token);
        if (await poisonEventStore.IsKeyPoisoned(groupId, @event.TopicPartitionOffset.Topic, @event.Key, token))
            return;

        var keys = await GetTopicPartitionKeys(@event.TopicPartitionOffset.TopicPartition, token);
        keys.Remove(DeserializeKey(@event.Key.Span));
    }

    private async ValueTask<HashSet<Guid>> GetTopicPartitionKeys(TopicPartition topicPartition, CancellationToken token)
    {
        if (!_partitionPoisonKeys.TryGetValue(topicPartition, out var keysTask))
            throw new Exception("Partition disabled");

        if (!keysTask.IsCompleted)
            await keysTask.WaitAsync(token);

        return keysTask.Result;
    }
}