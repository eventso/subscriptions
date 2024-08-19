using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Frozen;
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
    private readonly ConcurrentDictionary<string, TopicPoisonKeysCollection> _topicPoisonKeys =
        new ConcurrentDictionary<string, TopicPoisonKeysCollection>(concurrencyLevel: 1, capacity: 1);

    public bool IsEnabled => true;

    public void Assign(TopicPartition topicPartition)
    {
        // no concurrency, same thread as consume
        var concurrentKeysCollections = _topicPoisonKeys.GetOrAdd(
            topicPartition.Topic,
            static topic => new TopicPoisonKeysCollection(topic));
        
        concurrentKeysCollections.Register(topicPartition.Partition, GetPoisonKeys);
        
        logger.PartitionAssign(groupId, topicPartition.Topic, topicPartition.Partition);

        async Task<IReadOnlyCollection<Guid>> GetPoisonKeys(CancellationToken token)
        {
            await Task.Yield();
            
            var poisonedKeys = new List<Guid>();

            var keysSource = poisonEventStore.GetPoisonedKeys(groupId, topicPartition, token);
            await foreach (var key in keysSource)
                poisonedKeys.Add(DeserializeKey(topicPartition.Topic, [], key)); // note: empty headers here

            logger.PartitionKeysAcquired(groupId, topicPartition.Topic, topicPartition.Partition);     

            return poisonedKeys;
        }
    }

    public void Revoke(TopicPartition topicPartition)
    {
        // no concurrency, same thread as consume
        if (_topicPoisonKeys.TryGetValue(topicPartition.Topic, out var topicKeys))
            topicKeys.Unregister(topicPartition.Partition);

        logger.PartitionRevoke(groupId, topicPartition.Topic, topicPartition.Partition);
    }

    public async Task<IKeySet<Event>> GetKeys(string topic, CancellationToken token)
    {
        var keySet = await GetTopicKeys(topic).GetKeys(token);
        return keySet;
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

        var topicKeys = GetTopicKeys(@event.TopicPartitionOffset.TopicPartition.Topic);

        await poisonEventStore.AddEvent(groupId, @event, failureTimestamp, failureReason, token);
        await topicKeys.Add(@event.TopicPartitionOffset.TopicPartition.Partition, key, token);
    }

    public async IAsyncEnumerable<ConsumeResult<byte[], byte[]>> Peek([EnumeratorCancellation] CancellationToken token)
    {
        bool needToProcess = true;

        while (needToProcess)
        {
            needToProcess = false;
            var topics = _topicPoisonKeys.Keys.ToArray();
            foreach (var topic in topics)
            {
                TopicPoisonKeysCollection.KeySet topicKeys;
                try
                {
                    topicKeys = await GetTopicKeys(topic).GetKeys(token);
                    if (topicKeys.IsEmpty())
                        continue;
                }
                catch (Exception)
                {
                    continue;
                }

                foreach (var partition in topicKeys.GetPartitions())
                {
                    var eventForRetrying = await retryScheduler.GetNextRetryTarget(
                        groupId,
                        new TopicPartition(topic, partition),
                        token);

                    if (eventForRetrying == null)
                        continue;

                    needToProcess = true;
                    yield return eventForRetrying;
                }
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

        var topicKeys = GetTopicKeys(@event.TopicPartitionOffset.TopicPartition.Topic);
        
        await poisonEventStore.RemoveEvent(groupId, @event.TopicPartitionOffset, token);
        if (await poisonEventStore.IsKeyPoisoned(groupId, @event.TopicPartitionOffset.Topic, @event.Message.Key, token))
            return;

        await topicKeys.Remove(@event.TopicPartitionOffset.TopicPartition.Partition, key, token);
    }

    private static Guid DeserializeKey(string topic, Headers headers, byte[]? keyBytes)
    {
        return KeyGuidDeserializer.Instance.Deserialize(
            keyBytes,
            keyBytes == null,
            new SerializationContext(MessageComponentType.Key, topic, headers));
    }

    private TopicPoisonKeysCollection GetTopicKeys(string topic)
    {
        return _topicPoisonKeys.TryGetValue(topic, out var topicKeys)
            ? topicKeys
            : throw new Exception($"{topic} disabled");
    }
}