using Confluent.Kafka;

namespace Eventso.Subscription.Kafka.DeadLetter;

public interface IPoisonEventQueue
{
    bool IsEnabled { get; }
    
    void Assign(TopicPartition topicPartition);
    void Revoke(TopicPartition topicPartition);
    
    ValueTask<bool> IsPoison(TopicPartition topicPartition, Guid key, CancellationToken token);

    Task Blame(PoisonEvent @event, DateTime failureTimestamp, string failureReason, CancellationToken token);
 
    Task Rehabilitate(PoisonEvent @event, CancellationToken token);
    
    IAsyncEnumerable<PoisonEvent> GetEventsForRetrying(CancellationToken token);
}