using Confluent.Kafka;

namespace Eventso.Subscription.Kafka.DeadLetter.Store;

public interface IPoisonEventManager
{
    ValueTask<bool> IsPoison(TopicPartition topicPartition, Guid key, CancellationToken token);

    void Enable(TopicPartition topicPartition);
    void Disable(TopicPartition topicPartition);

    Task Blame(
        PoisonEvent @event,
        DateTime failureTimestamp,
        string failureReason,
        CancellationToken token);
    
    IAsyncEnumerable<PoisonEvent> GetEventsForRetrying(CancellationToken token);
 
    Task Rehabilitate(PoisonEvent @event, CancellationToken token);
}