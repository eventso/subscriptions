using Confluent.Kafka;

namespace Eventso.Subscription.Kafka.DeadLetter;

public interface IPoisonEventQueue
{
    bool IsEnabled { get; }

    void Assign(TopicPartition topicPartition);
    void Revoke(TopicPartition topicPartition);
    
    ValueTask<bool> Contains(TopicPartition topicPartition, Guid key, CancellationToken token);

    Task Enqueue(ConsumeResult<byte[], byte[]> @event, DateTime failureTimestamp, string failureReason, CancellationToken token);
 
    Task Dequeue(ConsumeResult<byte[], byte[]> @event, CancellationToken token);
    
    IAsyncEnumerable<ConsumeResult<byte[], byte[]>> Peek(CancellationToken token);
}