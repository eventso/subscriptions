using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Eventso.Subscription.Kafka.DeadLetter.Store;
using Microsoft.Extensions.Logging;

namespace Eventso.Subscription.Kafka.DeadLetter
{
    public sealed class TopicRetryingService
    {
        private readonly string _topic;
        private readonly IPoisonEventStore _poisonEventStore;
        private readonly IDeserializer<ConsumedMessage> _deserializer;
        private readonly IEventHandler<Event> _eventHandler;
        private readonly ILogger<TopicRetryingService> _logger;

        public TopicRetryingService(
            string topic,
            IPoisonEventStore poisonEventStore,
            IDeserializer<ConsumedMessage> deserializer,
            IEventHandler<Event> eventHandler,
            ILogger<TopicRetryingService> logger)
        {
            _topic = topic;
            _poisonEventStore = poisonEventStore;
            _deserializer = deserializer;
            _eventHandler = eventHandler;
            _logger = logger;
        }

        public async Task Retry(CancellationToken cancellationToken)
        {
            using var retryScope = _logger.BeginScope(
                new[] { new KeyValuePair<string, string>("eventso_retry_topic", _topic) });

            _logger.LogInformation($"Started event retrying.");

            using var events = new PooledList<Event>(4);

            await foreach (var storedEvent in _poisonEventStore.GetEventsForRetrying(_topic, cancellationToken))
            {
                var @event = Deserialize(storedEvent);
                events.Add(@event);

                _logger.LogInformation($"Queued event {@event.GetTopicPartitionOffset()} for retrying.");
            }

            await _eventHandler.Handle(events, cancellationToken);

            _logger.LogInformation($"Finished event retrying.");
        }

        private Event Deserialize(StoredPoisonEvent storedEvent)
        {
            var headers = new Headers();
            foreach (var (key, data) in storedEvent.Headers)
                headers.Add(key, data);

            var consumeResult = new ConsumeResult<Guid, ConsumedMessage>
            {
                // shaky and depends on Confluent.Kafka contract
                Message = new Message<Guid, ConsumedMessage>
                {
                    Key = storedEvent.Key,
                    Value = _deserializer.Deserialize(storedEvent.Value.Span, storedEvent.Value.IsEmpty, SerializationContext.Empty),
                    Timestamp = new Timestamp(storedEvent.CreationTimestamp, TimestampType.NotAvailable),
                    Headers = headers
                },
                IsPartitionEOF = false,
                TopicPartitionOffset = storedEvent.TopicPartitionOffset
            };

            return new Event(consumeResult, storedEvent.TopicPartitionOffset.Topic);
        }
    }
}