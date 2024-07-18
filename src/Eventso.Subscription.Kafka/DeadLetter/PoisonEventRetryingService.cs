using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Eventso.Subscription.Kafka.DeadLetter;

public sealed class PoisonEventRetryingService(
    string groupId,
    IDeserializer<ConsumedMessage> deserializer,
    IReadOnlyDictionary<string, Observing.EventHandler<Event>> eventHandlers,
    IPoisonEventQueue poisonEventQueue,
    ILogger<PoisonEventRetryingService> logger)
{
    public async Task Retry(ConsumeResult<byte[], byte[]> poisonEvent, CancellationToken token)
    {
        var @event = Deserialize(poisonEvent);
        logger.RetryStarted(groupId, poisonEvent.TopicPartitionOffset);

        try
        {
            await eventHandlers[@event.Topic].Handle(@event, new HandlingContext(), token);
            logger.RetrySuccessful(groupId, poisonEvent.TopicPartitionOffset);
        }
        catch (Exception exception)
        {
            await poisonEventQueue.Enqueue(poisonEvent, DateTime.UtcNow, exception.ToString(), token);
            
            logger.RetryFailed(exception, groupId, poisonEvent.TopicPartitionOffset);
            return;
        }

        await poisonEventQueue.Dequeue(poisonEvent, token);
    }

    private Event Deserialize(ConsumeResult<byte[], byte[]> @event)
    {
        var consumeResult = new ConsumeResult<Guid, ConsumedMessage>
        {
            Message = new Message<Guid, ConsumedMessage>
            {
                Key = KeyGuidDeserializer.Instance.Deserialize(
                    @event.Message.Key,
                    @event.Message.Key.Length == 0,
                    new SerializationContext(MessageComponentType.Key, @event.Topic, @event.Message.Headers)),
                Value = deserializer.Deserialize(
                    @event.Message.Value,
                    @event.Message.Value.Length == 0,
                    new SerializationContext(MessageComponentType.Value, @event.Topic, @event.Message.Headers)),
                Timestamp = @event.Message.Timestamp,
                Headers = @event.Message.Headers
            },
            IsPartitionEOF = false,
            TopicPartitionOffset = @event.TopicPartitionOffset
        };

        return new Event(consumeResult);
    }
}