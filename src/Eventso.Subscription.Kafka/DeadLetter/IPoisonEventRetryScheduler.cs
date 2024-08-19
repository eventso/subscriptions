using Confluent.Kafka;

namespace Eventso.Subscription.Kafka.DeadLetter;

public interface IPoisonEventRetryScheduler
{
    Task<ConsumeResult<byte[], byte[]>?> GetNextRetryTarget(
        string groupId,
        TopicPartition topicPartition,
        CancellationToken token);
}