using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Eventso.Subscription.Kafka;

public sealed class LoggingConsumerEventsObserver(ILogger<KafkaConsumer> logger, IConsumerEventsObserver next) : IConsumerEventsObserver
{
    public void OnAssign(IReadOnlyCollection<TopicPartition> topicPartitions)
    {
        logger.RebalancePartitionsAssigned(topicPartitions);
        next.OnAssign(topicPartitions);
    }

    public void OnRevoke(IReadOnlyCollection<TopicPartitionOffset> topicPartitions)
    {
        logger.RebalancePartitionsRevoked(topicPartitions);
        next.OnRevoke(topicPartitions);
    }

    public void OnClose(IReadOnlyCollection<TopicPartition> topicPartitions)
    {
        logger.ConsumerClosed(topicPartitions);
        next.OnClose(topicPartitions);
    }

    public Task<bool> TryHandleSerializationException(ConsumeException exception, CancellationToken token)
    {
        logger.SerializationError(exception, exception.ConsumerRecord?.TopicPartitionOffset);
        return next.TryHandleSerializationException(exception, token);
    }
}