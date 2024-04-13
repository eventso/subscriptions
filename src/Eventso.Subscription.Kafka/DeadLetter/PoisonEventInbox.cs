using System.Runtime.InteropServices;
using Confluent.Kafka;
using Eventso.Subscription.Kafka.DeadLetter.Store;
using Eventso.Subscription.Observing.DeadLetter;
using Microsoft.Extensions.Logging;

namespace Eventso.Subscription.Kafka.DeadLetter;

public sealed class PoisonEventInbox : IPoisonEventInbox<Event>, IPoisonEventInboxInitializer, IDisposable
{
    private readonly IPoisonEventStore _eventStore;
    private readonly int _maxNumberOfPoisonedEventsInTopic;
    private readonly ThreadSafeConsumer _deadMessageConsumer;
    private readonly Dictionary<string, HashSet<Guid>> _poisonKeysByTopic;

    public PoisonEventInbox(
        IPoisonEventStore eventStore,
        int maxNumberOfPoisonedEventsInTopic,
        KafkaConsumerSettings settings,
        string[] topics,
        ILogger<PoisonEventInbox> logger)
    {
        _eventStore = eventStore;
        _maxNumberOfPoisonedEventsInTopic = maxNumberOfPoisonedEventsInTopic;
        _deadMessageConsumer = new ThreadSafeConsumer(settings, topics, logger);
        _poisonKeysByTopic = topics.ToDictionary(t => t, _ => new HashSet<Guid>(maxNumberOfPoisonedEventsInTopic));
    }

    public async Task Add(PoisonEvent<Event> @event, CancellationToken cancellationToken)
    {
        await EnsureThreshold(@event.Event.Topic, cancellationToken);
        await _eventStore.Add(
            DateTime.UtcNow,
            CreateOpeningPoisonEvent(@event, cancellationToken),
            cancellationToken);
        _poisonKeysByTopic[@event.Event.Topic].Add(@event.Event.GetKey());
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
        foreach (var @event in events)
            _poisonKeysByTopic[@event.Event.Topic].Add(@event.Event.GetKey());
    }

    public Task<bool> IsPartOfPoisonStream(Event @event, CancellationToken cancellationToken)
        => Task.FromResult(_poisonKeysByTopic[@event.Topic].Contains(@event.GetKey()));

    public Task<IPoisonStreamCollection<Event>?> GetPoisonStreams(IReadOnlyCollection<Event> events, CancellationToken cancellationToken)
    {
        HashSet<StreamId>? poisonStreamIds = null;
        foreach (var @event in events)
        {
            if (!_poisonKeysByTopic[@event.Topic].Contains(@event.GetKey()))
                continue;

            poisonStreamIds ??= new HashSet<StreamId>();
            poisonStreamIds.Add(new StreamId(@event.Topic, @event.GetKey()));
        }

        return Task.FromResult(
            poisonStreamIds != null
                ? new PoisonStreamCollection(poisonStreamIds)
                : default(IPoisonStreamCollection<Event>));
    }

    public void Dispose()
    {
        _deadMessageConsumer.Close();
    }

    private OpeningPoisonEvent CreateOpeningPoisonEvent(
        PoisonEvent<Event> @event,
        CancellationToken cancellationToken)
    {
        var topicPartitionOffset = @event.Event.GetTopicPartitionOffset();

        var rawEvent = _deadMessageConsumer.Consume(topicPartitionOffset, cancellationToken);

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


    private async Task EnsureThreshold(string topic, CancellationToken cancellationToken)
    {
        var alreadyPoisoned = await _eventStore.Count(topic, cancellationToken);
        if (alreadyPoisoned >= _maxNumberOfPoisonedEventsInTopic)
            throw new EventHandlingException(
                topic,
                $"Dead letter queue exceeds {_maxNumberOfPoisonedEventsInTopic} size.",
                null);
    }

    [StructLayout(LayoutKind.Auto)]
    private struct InboxThresholdChecker
    {
        private readonly PoisonEventInbox _inbox;

        private string? _singleTopic;
        private List<string>? _topics;

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
}