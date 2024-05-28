using System.Runtime.CompilerServices;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Eventso.Subscription.Kafka.DeadLetter;

public sealed class PoisonEventQueue(
    IPoisonEventStore poisonEventStore,
    IPoisonEventRetryScheduler retryScheduler,
    string groupId,
    int maxNumberOfPoisonedEventsInTopic,
    ILogger<PoisonEventQueue> logger)
    : IPoisonEventQueue
{
    private readonly Dictionary<TopicPartition, Task<HashSet<Guid>>> _partitionPoisonKeys = new();

    public bool IsEnabled => true;

    public void Assign(TopicPartition topicPartition)
    {
        // no concurrency, same thread as consume
        _partitionPoisonKeys[topicPartition] = GetPoisonKeys();
        
        logger.PartitionAssign(groupId, topicPartition.Topic, topicPartition.Partition);

        async Task<HashSet<Guid>> GetPoisonKeys()
        {
            var poisonedKeys = new HashSet<Guid>();

            var keysSource = poisonEventStore.GetPoisonedKeys(groupId, topicPartition, CancellationToken.None);
            await foreach (var key in keysSource)
                poisonedKeys.Add(DeserializeKey(topicPartition.Topic, [], key)); // note: empty headers here
            return poisonedKeys;
        }
    }

    public void Revoke(TopicPartition topicPartition)
    {
        // no concurrency, same thread as consume
        _partitionPoisonKeys.Remove(topicPartition);
        
        logger.PartitionRevoke(groupId, topicPartition.Topic, topicPartition.Partition);
    }

    public async ValueTask<bool> Contains(TopicPartition topicPartition, Guid key, CancellationToken token)
    {
        var keys = await GetTopicPartitionKeys(topicPartition, token);
        return keys.Contains(key);
    }

    public async Task Enqueue(ConsumeResult<byte[], byte[]> @event, DateTime failureTimestamp, string failureReason, CancellationToken token)
    {
        var key = DeserializeKey(@event.Topic, @event.Message.Headers, @event.Message.Key);
        logger.Enqueue(
            groupId,
            @event.TopicPartitionOffset.Topic,
            @event.TopicPartitionOffset.Partition,
            @event.TopicPartitionOffset.Offset,
            key,
            failureReason);
        
        var alreadyPoisoned = await poisonEventStore.CountPoisonedEvents(groupId, @event.TopicPartitionOffset.Topic, token);
        if (alreadyPoisoned >= maxNumberOfPoisonedEventsInTopic)
            throw new EventHandlingException(
                @event.TopicPartitionOffset.Topic,
                $"Dead letter queue size ({alreadyPoisoned}) exceeds threshold ({maxNumberOfPoisonedEventsInTopic}). " +
                $"Poison event: {@event.TopicPartitionOffset}. " +
                $"Error: {failureReason}",
                null);
        
        await poisonEventStore.AddEvent(groupId, @event, failureTimestamp, failureReason, token);
        
        var keys = await GetTopicPartitionKeys(@event.TopicPartitionOffset.TopicPartition, token);
        keys.Add(key);
    }

    public async IAsyncEnumerable<ConsumeResult<byte[], byte[]>> Peek([EnumeratorCancellation] CancellationToken token)
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

                var eventForRetrying = await retryScheduler.GetNextRetryTarget(groupId, topicPartition, token);
                if (eventForRetrying == null)
                    continue;

                needToProcess = true;
                yield return eventForRetrying;
            }
        }
    }

    public async Task Dequeue(ConsumeResult<byte[], byte[]> @event, CancellationToken token)
    {
        var key = DeserializeKey(@event.Topic, @event.Message.Headers, @event.Message.Key);
        logger.Dequeue(
            groupId,
            @event.TopicPartitionOffset.Topic,
            @event.TopicPartitionOffset.Partition,
            @event.TopicPartitionOffset.Offset,
            key);
        await poisonEventStore.RemoveEvent(groupId, @event.TopicPartitionOffset, token);
        if (await poisonEventStore.IsKeyPoisoned(groupId, @event.TopicPartitionOffset.Topic, @event.Message.Key, token))
            return;

        var keys = await GetTopicPartitionKeys(@event.TopicPartitionOffset.TopicPartition, token);
        keys.Remove(key);
    }

    private static Guid DeserializeKey(string topic, Headers headers, byte[]? keyBytes)
    {
        return KeyGuidDeserializer.Instance.Deserialize(
            keyBytes,
            keyBytes == null,
            new SerializationContext(MessageComponentType.Key, topic, headers));
    }

    private async ValueTask<HashSet<Guid>> GetTopicPartitionKeys(TopicPartition topicPartition, CancellationToken token)
    {
        if (!_partitionPoisonKeys.TryGetValue(topicPartition, out var keysTask))
            throw new Exception($"{topicPartition} disabled");

        if (!keysTask.IsCompleted)
        {
            await keysTask.WaitAsync(token);
            logger.PartitionKeysAcquired(groupId, topicPartition.Topic, topicPartition.Partition);            
        }

        return keysTask.Result;
    }
}