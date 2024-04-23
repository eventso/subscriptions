using Confluent.Kafka;
using Eventso.Subscription.Observing.DeadLetter;
using Microsoft.Extensions.Logging;

namespace Eventso.Subscription.Kafka.DeadLetter;

public sealed class TopicRetryingService(
    string groupId,
    string[] topics,
    IDeserializer<ConsumedMessage> deserializer,
    IReadOnlyDictionary<string, Observing.EventHandler<Event>> eventHandlers,
    IDeadLetterQueueScopeFactory deadLetterQueueScopeFactory,
    IPoisonEventQueue poisonEventQueue,
    ILogger<TopicRetryingService> logger)
{
    public async Task Retry(CancellationToken token)
    {
        using var retryScope = logger.BeginScope(
            new[] { new KeyValuePair<string, string>("eventso_retry_topic", string.Join(",", topics)) });

        logger.LogInformation("Started event retrying");

        await foreach (var @event in poisonEventQueue.GetEventsForRetrying(token))
            await Retry(@event, token);

        logger.LogInformation("Finished event retrying");
    }

    private async Task Retry(PoisonEvent poisonEvent, CancellationToken cancellationToken)
    {
        var @event = Deserialize(poisonEvent);
        logger.LogInformation(
            "Retrying event {TopicPartitionOffset} in group {GroupId} successfully",
            poisonEvent.TopicPartitionOffset,
            groupId);

        try
        {
            await Handle(@event, cancellationToken);
            logger.LogInformation(
                "Retried event {TopicPartitionOffset} in group {GroupId} successfully",
                poisonEvent.TopicPartitionOffset,
                groupId);
        }
        catch (Exception exception)
        {
            await poisonEventQueue.Blame(
                //todo looks bad, think how deal with failure count in a better way
                poisonEvent with { FailureCount = poisonEvent.FailureCount + 1 },
                DateTime.UtcNow,
                exception.ToString(),
                cancellationToken);
            
            logger.LogInformation(
                "Retried event {TopicPartitionOffset} in group {GroupId} without success",
                poisonEvent.TopicPartitionOffset,
                groupId);
            return;
        }

        await poisonEventQueue.Rehabilitate(poisonEvent, cancellationToken);
    }

    private Event Deserialize(PoisonEvent @event)
    {
        var headers = new Headers();
        foreach (var header in @event.Headers)
            headers.Add(header.Key, header.Data.ToArray());

        var consumeResult = new ConsumeResult<Guid, ConsumedMessage>
        {
            // shaky and depends on Confluent.Kafka contract
            Message = new Message<Guid, ConsumedMessage>
            {
                Key = KeyGuidDeserializer.Instance.Deserialize(@event.Key.Span, @event.Key.IsEmpty, SerializationContext.Empty),
                Value = deserializer.Deserialize(@event.Value.Span, @event.Value.IsEmpty, SerializationContext.Empty),
                Timestamp = new Timestamp(@event.CreationTimestamp, TimestampType.NotAvailable),
                Headers = headers
            },
            IsPartitionEOF = false,
            TopicPartitionOffset = @event.TopicPartitionOffset
        };

        return new Event(consumeResult);
    }

    private async Task Handle(Event @event, CancellationToken cancellationToken)
    {
        using var dlqScope = deadLetterQueueScopeFactory.Create(@event);

        await eventHandlers[@event.Topic].Handle(@event, cancellationToken);

        var poisonEvents = dlqScope.GetPoisonEvents();
        if (poisonEvents.Count > 0)
            throw new Exception(poisonEvents.Single().Reason);
    }
}