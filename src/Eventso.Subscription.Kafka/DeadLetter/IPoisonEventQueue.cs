using Confluent.Kafka;

namespace Eventso.Subscription.Kafka.DeadLetter;

public interface IPoisonEventQueue
{
    // manager
    bool IsEnabled { get; }

    // manager
    void Assign(TopicPartition topicPartition);
    
    // manager
    void Revoke(TopicPartition topicPartition);

    // inbox
    Task<IKeySet<Event>> GetKeys(string topic, CancellationToken token);

    Task<bool> IsLimitReached(TopicPartition topicPartition, CancellationToken token);

    // queue
    Task Enqueue(ConsumeResult<byte[], byte[]> @event, DateTime failureTimestamp, string failureReason, CancellationToken token);
 
    // queue
    Task Dequeue(ConsumeResult<byte[], byte[]> @event, CancellationToken token);
    
    // queue
    IAsyncEnumerable<ConsumeResult<byte[], byte[]>> Peek(CancellationToken token);
}