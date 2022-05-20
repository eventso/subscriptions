using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Eventso.Subscription.Kafka.DeadLetter.Store;
using Eventso.Subscription.Observing.DeadLetter;
using Microsoft.Extensions.Logging;

namespace Eventso.Subscription.Kafka.DeadLetter
{
    public sealed class PoisonEventInbox : IPoisonEventInbox<Event>, IPoisonRecordInbox, IDisposable
    {
        private readonly IPoisonEventStore _eventStore;
        private readonly IConsumer<byte[], byte[]> _deadMessageConsumer;

        public PoisonEventInbox(
            IPoisonEventStore eventStore,
            ConsumerSettings settings,
            ILogger<PoisonEventInbox> logger)
        {
            _eventStore = eventStore;

            if (string.IsNullOrWhiteSpace(settings.Config.BootstrapServers))
                throw new InvalidOperationException("Brokers are not specified.");

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
            _deadMessageConsumer = new ConsumerBuilder<byte[], byte[]>(config)
                .SetKeyDeserializer(Deserializers.ByteArray)
                .SetValueDeserializer(Deserializers.ByteArray)
                .SetErrorHandler((_, e) => logger.LogError(
                    $"{nameof(PoisonEventInbox)} internal error: Topic: {settings.Topic}, {e.Reason}, Fatal={e.IsFatal}," +
                    $" IsLocal= {e.IsLocalError}, IsBroker={e.IsBrokerError}"))
                .Build();
        }

        public async Task Add(
            ConsumeResult<byte[], byte[]> consumeResult,
            string failureReason,
            CancellationToken token)
        {
            await EnsureThreshold(consumeResult.Topic, token);
            await _eventStore.Add(
                DateTime.UtcNow,
                CreateOpeningPoisonEvent(consumeResult, failureReason),
                token);
        }

        public async Task Add(PoisonEvent<Event> @event, CancellationToken cancellationToken)
        {
            await EnsureThreshold(@event.Event.Topic, cancellationToken);
            await _eventStore.Add(
                DateTime.UtcNow,
                CreateOpeningPoisonEvent(@event, cancellationToken),
                cancellationToken);
        }

        public async Task Add(IReadOnlyCollection<PoisonEvent<Event>> events, CancellationToken cancellationToken)
        {
            if (events.Count == 0)
                return;

            var inboxThresholdChecker = new InboxThresholdChecker(this);
            var openingPoisonEvents = new PooledList<OpeningPoisonEvent>(events.Count);
            foreach (var @event in events)
            {
                await inboxThresholdChecker.EnsureThreshold(@event.Event.Topic, cancellationToken);
                openingPoisonEvents.Add(CreateOpeningPoisonEvent(@event, cancellationToken));
            }

            await _eventStore.Add(DateTime.UtcNow, openingPoisonEvents, cancellationToken);
        }

        public Task<bool> IsPartOfPoisonStream(Event @event, CancellationToken cancellationToken)
            => _eventStore.IsStreamStored(@event.Topic, @event.GetKey(), cancellationToken);

        public async Task<IPoisonStreamCollection<Event>> GetPoisonStreams(
            IReadOnlyCollection<Event> events,
            CancellationToken cancellationToken)
        {
            using var streamIds = new PooledList<StreamId>(events.Count);
            foreach (var @event in events)
                streamIds.Add(new StreamId(@event.Topic, @event.GetKey()));
            
            HashSet<StreamId> poisonStreamIds = null;
            await foreach (var streamId in _eventStore.GetStoredStreams(streamIds, cancellationToken))
            {
                poisonStreamIds ??= new HashSet<StreamId>();
                poisonStreamIds.Add(streamId);
            }

            return poisonStreamIds != null
                ? new PoisonStreamCollection(poisonStreamIds)
                : null;
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
            var topicPartitionOffset = @event.Event.GetTopicPartitionOffset();

            var rawEvent = Consume(topicPartitionOffset, cancellationToken);

            return CreateOpeningPoisonEvent(rawEvent, @event.Reason);
        }

        private static OpeningPoisonEvent CreateOpeningPoisonEvent(
            ConsumeResult<byte[], byte[]> poisonRecord,
            string failureReason)
        {
            return new OpeningPoisonEvent(
                poisonRecord.TopicPartitionOffset,
                KeyGuidDeserializer.Instance.Deserialize(
                    poisonRecord.Message.Key,
                    poisonRecord.Message.Key.Length == 0,
                    SerializationContext.Empty),
                poisonRecord.Message.Value,
                poisonRecord.Message.Timestamp.UtcDateTime,
                poisonRecord.Message
                    .Headers
                    .Select(c => new EventHeader(c.Key, c.GetValueBytes()))
                    .ToArray(),
                failureReason);
        }

        private ConsumeResult<byte[], byte[]> Consume(
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


        // TODO const -> settings
        private const int MaxNumberOfPoisonedEventsInTopic = 1000;
        private async Task EnsureThreshold(string topic, CancellationToken cancellationToken)
        {
            var alreadyPoisoned = await _eventStore.Count(topic, cancellationToken);
            if (alreadyPoisoned >= MaxNumberOfPoisonedEventsInTopic)
                throw new EventHandlingException(
                    topic,
                    $"Dead letter queue exceeds {MaxNumberOfPoisonedEventsInTopic} size.",
                    null);
        }

        [StructLayout(LayoutKind.Auto)]
        private struct InboxThresholdChecker
        {
            private readonly PoisonEventInbox _inbox;

            private string _singleTopic;
            private List<string> _topics;

            public InboxThresholdChecker(PoisonEventInbox inbox)
            {
                _inbox = inbox;
                _singleTopic = null;
                _topics = null;
            }

            public async ValueTask EnsureThreshold(string topic, CancellationToken cancellationToken)
            {
                if (_singleTopic == topic)
                    return;

                if (_topics?.Contains(topic) == true)
                    return;

                await _inbox.EnsureThreshold(topic, cancellationToken);

                if (_singleTopic == null)
                {
                    _singleTopic = topic;
                    return;
                }

                _topics ??= new List<string>(1);
                _topics.Add(topic);
            }
        }
        
        private sealed class PoisonStreamCollection : IPoisonStreamCollection<Event>
        {
            private readonly HashSet<StreamId> _poisonStreamIds;

            public PoisonStreamCollection(HashSet<StreamId> poisonStreamIds)
                => _poisonStreamIds = poisonStreamIds;

            public bool IsPartOfPoisonStream(Event @event)
                => _poisonStreamIds.Contains(new StreamId(@event.Topic, @event.GetKey()));
        }
    }
}