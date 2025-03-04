using Confluent.Kafka;

namespace Eventso.Subscription.Kafka.DeadLetter;

public sealed class PoisonEventQueueConsumerObserver(IPoisonEventQueue queue) : IConsumerEventsObserver
{
    public void OnAssign(IReadOnlyCollection<TopicPartition> topicPartitions)
    {
        foreach (var item in topicPartitions)
            queue.Assign(item);
    }

    public void OnRevoke(IReadOnlyCollection<TopicPartitionOffset> topicPartitions)
    {
        foreach (var item in topicPartitions)
            queue.Revoke(item.TopicPartition);
    }

    public void OnClose(IReadOnlyCollection<TopicPartition> topicPartitions)
    {
        foreach (var item in topicPartitions)
            queue.Revoke(item);
    }

    public async Task<bool> TryHandleSerializationException(ConsumeException exception, CancellationToken token)
    {
        await queue.Enqueue(exception.ConsumerRecord, DateTime.UtcNow, exception.Message, token);

        return true;
    }
}