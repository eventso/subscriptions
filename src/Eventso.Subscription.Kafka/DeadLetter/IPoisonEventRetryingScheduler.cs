using Confluent.Kafka;

namespace Eventso.Subscription.Kafka.DeadLetter;

public interface IPoisonEventRetryingScheduler
{
    Task<PoisonEvent?> GetEventForRetrying(string groupId, TopicPartition topicPartition, CancellationToken token);
}