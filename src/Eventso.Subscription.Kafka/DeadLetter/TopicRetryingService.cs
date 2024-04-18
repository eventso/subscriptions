using Confluent.Kafka;
using Eventso.Subscription.Observing.DeadLetter;
using Microsoft.Extensions.Logging;

namespace Eventso.Subscription.Kafka.DeadLetter;

public sealed class TopicRetryingService
{
    private readonly string[] _topics;
    private readonly IDeserializer<ConsumedMessage> _deserializer;
    private readonly IReadOnlyDictionary<string, Observing.EventHandler<Event>> _eventHandlers;
    private readonly IDeadLetterQueueScopeFactory _deadLetterQueueScopeFactory;
    private readonly IPoisonEventQueue _poisonEventQueue;
    private readonly ILogger<TopicRetryingService> _logger;

    public TopicRetryingService(
        string[] topics,
        IDeserializer<ConsumedMessage> deserializer,
        IReadOnlyDictionary<string, Observing.EventHandler<Event>> eventHandlers,
        IDeadLetterQueueScopeFactory deadLetterQueueScopeFactory,
        IPoisonEventQueue poisonEventQueue,
        ILogger<TopicRetryingService> logger)
    {
        _topics = topics;
        _deserializer = deserializer;
        _eventHandlers = eventHandlers;
        _deadLetterQueueScopeFactory = deadLetterQueueScopeFactory;
        _poisonEventQueue = poisonEventQueue;
        _logger = logger;
    }

    public async Task Retry(CancellationToken cancellationToken)
    {
        using var retryScope = _logger.BeginScope(
            new[] { new KeyValuePair<string, string>("eventso_retry_topic", string.Join(",", _topics)) });

        _logger.LogInformation("Started event retrying.");

        await foreach (var storedEvent in _poisonEventQueue.GetEventsForRetrying(cancellationToken))
        {
            await Retry(storedEvent, cancellationToken);
            _logger.LogInformation("Retried event {TopicPartitionOffset}.", storedEvent.TopicPartitionOffset);
        }

        _logger.LogInformation("Finished event retrying.");
    }

    private async Task Retry(PoisonEvent poisonEvent, CancellationToken cancellationToken)
    {
        var @event = Deserialize(poisonEvent);

        try
        {
            await Handle(@event, cancellationToken);
        }
        catch (Exception exception)
        {
            await _poisonEventQueue.Blame(
                // looks bad, think how deal with failure count in a better way
                poisonEvent with { FailureCount = poisonEvent.FailureCount + 1 },
                DateTime.UtcNow,
                exception.ToString(),
                cancellationToken);
            return;
        }

        await _poisonEventQueue.Rehabilitate(poisonEvent, cancellationToken);
    }

    private Event Deserialize(PoisonEvent storedEvent)
    {
        var headers = new Headers();
        foreach (var header in storedEvent.Headers)
            headers.Add(header.Key, header.Data.ToArray());

        var consumeResult = new ConsumeResult<Guid, ConsumedMessage>
        {
            // shaky and depends on Confluent.Kafka contract
            Message = new Message<Guid, ConsumedMessage>
            {
                Key = KeyGuidDeserializer.Instance.Deserialize(storedEvent.Key.Span, storedEvent.Key.IsEmpty, SerializationContext.Empty),
                Value = _deserializer.Deserialize(storedEvent.Value.Span, storedEvent.Value.IsEmpty, SerializationContext.Empty),
                Timestamp = new Timestamp(storedEvent.CreationTimestamp, TimestampType.NotAvailable),
                Headers = headers
            },
            IsPartitionEOF = false,
            TopicPartitionOffset = storedEvent.TopicPartitionOffset,
        };

        return new Event(consumeResult);
    }

    private async Task Handle(Event @event, CancellationToken cancellationToken)
    {
        using var dlqScope = _deadLetterQueueScopeFactory.Create(@event);

        await _eventHandlers[@event.Topic].Handle(@event, cancellationToken);

        var poisonEvents = dlqScope.GetPoisonEvents();
        if (poisonEvents.Count > 0)
            throw new Exception(poisonEvents.Single().Reason);
    }
}