using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Eventso.Subscription.Kafka.DeadLetter.Store;
using Eventso.Subscription.Observing.DeadLetter;
using Microsoft.Extensions.Logging;

namespace Eventso.Subscription.Kafka.DeadLetter
{
    public sealed class PoisonEventInbox : IPoisonEventInbox<Event>, IDisposable
    {
        // TODO const -> settings
        private const int MaxNumberOfPoisonedEventsInTopic = 1000;

        private readonly IPoisonEventStore _eventStore;
        private readonly ILogger<PoisonEventInbox> _logger;
        private readonly IConsumer<Guid, byte[]> _deadMessageConsumer;
        private readonly string _topic;

        public PoisonEventInbox(
            IPoisonEventStore eventStore,
            ConsumerSettings settings,
            ILogger<PoisonEventInbox> logger)
        {
            _eventStore = eventStore;
            _logger = logger;
            _topic = settings.Topic;

            if (string.IsNullOrWhiteSpace(settings.Config.BootstrapServers))
                throw new InvalidOperationException("Brokers are not specified.");

            if (string.IsNullOrEmpty(settings.Topic))
                throw new InvalidOperationException("Topics are not specified.");

            if (string.IsNullOrEmpty(settings.Config.GroupId))
                throw new InvalidOperationException("Group Id is not specified.");

            var config = new ConsumerConfig(settings.Config.ToDictionary(e => e.Key, e => e.Value))
            {
                EnableAutoCommit = false,
                EnableAutoOffsetStore = false,
                AutoOffsetReset = AutoOffsetReset.Error,
                AllowAutoCreateTopics = false,
                GroupId = settings.Config.GroupId + "_cemetery" // boo!
            };
            _deadMessageConsumer = new ConsumerBuilder<Guid, byte[]>(config)
                .SetKeyDeserializer(KeyGuidDeserializer.Instance)
                .SetValueDeserializer(Deserializers.ByteArray)
                .SetErrorHandler((_, e) => logger.LogError(
                    $"{nameof(PoisonEventInbox)} internal error: Topic: {settings.Topic}, {e.Reason}, Fatal={e.IsFatal}," +
                    $" IsLocal= {e.IsLocalError}, IsBroker={e.IsBrokerError}"))
                .Build();
        }

        public async Task Add(IReadOnlyCollection<PoisonEvent<Event>> events, CancellationToken cancellationToken)
        {
            if (events.Count == 0)
                return;

            await EnsureInboxThreshold(cancellationToken);

            foreach (var @event in events)
                _logger.LogInformation($"event {new TopicPartitionOffset(@event.Event.Topic, @event.Event.Partition, @event.Event.Offset)} is first poison");

            await _eventStore.Add(
                _topic,
                DateTime.UtcNow,
                events.Select(e => CreateOpeningPoisonEvent(e, cancellationToken)).ToArray(),
                cancellationToken);
        }

        private async Task EnsureInboxThreshold(CancellationToken cancellationToken)
        {
            var alreadyPoisoned = await _eventStore.Count(_topic, cancellationToken);
            if (alreadyPoisoned < MaxNumberOfPoisonedEventsInTopic)
                return;

            throw new EventHandlingException(
                _topic,
                $"Dead letter queue exceeds {MaxNumberOfPoisonedEventsInTopic} size.",
                null);
        }

        public Task<bool> Contains(string topic, Guid key, CancellationToken cancellationToken)
            => _eventStore.IsKeyStored(topic, key, cancellationToken);

        public async Task<IReadOnlySet<Guid>> GetContainedKeys(
            string topic,
            IReadOnlyCollection<Guid> keys,
            CancellationToken cancellationToken)
        {
            var result = new HashSet<Guid>();
            await foreach (var storedKey in _eventStore.GetStoredKeys(topic, keys, cancellationToken))
                result.Add(storedKey);
            return result;
        }

        public void Dispose()
        {
            _deadMessageConsumer.Close();
            _deadMessageConsumer.Dispose();
        }

        private OpeningPoisonEvent CreateOpeningPoisonEvent(
            PoisonEvent<Event> @event,
            CancellationToken cancellationToken)
        {
            var rawEvent = Consume(
                new TopicPartitionOffset(@event.Topic, @event.Event.Partition, @event.Event.Offset), 
                cancellationToken);

            return new OpeningPoisonEvent(
                new PartitionOffset(@event.Event.Partition, @event.Event.Offset),
                rawEvent.Message.Key,
                rawEvent.Message.Value,
                rawEvent.Message.Timestamp.UtcDateTime,
                rawEvent.Message
                    .Headers
                    .Select(c => new EventHeader(c.Key, c.GetValueBytes()))
                    .ToArray(),
                @event.Reason);
        }

        private ConsumeResult<Guid, byte[]> Consume(
            TopicPartitionOffset topicPartitionOffset,
            CancellationToken cancellationToken)
        {
            try
            {
                // one per observer (so no concurrency should exist) 
                _deadMessageConsumer.Assign(topicPartitionOffset);

                var rawEvent = _deadMessageConsumer.Consume(cancellationToken);
                if (!rawEvent.TopicPartitionOffset.Equals(topicPartitionOffset))
                    throw new EventHandlingException(
                        topicPartitionOffset.ToString(),
                        "Consumed message offset doesn't match requested one.",
                        null);

                return rawEvent;
            }
            finally
            {
                _deadMessageConsumer.Unassign();
            }
        }
    }
}