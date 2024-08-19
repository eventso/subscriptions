using Confluent.Kafka;
using Eventso.Subscription.Observing.DeadLetter;
using Microsoft.Extensions.Logging;

namespace Eventso.Subscription.Kafka.DeadLetter;

public sealed class PoisonEventInbox(
    IPoisonEventQueue poisonEventQueue,
    KafkaConsumerSettings settings,
    string topic,
    ILogger<PoisonEventInbox> logger)
    : IPoisonEventInbox<Event>, IDisposable
{
    private readonly Lazy<ThreadSafeConsumer> _deadMessageConsumer = new(
        () => new ThreadSafeConsumer(settings, topic, logger),
        LazyThreadSafetyMode.ExecutionAndPublication);

    public Task<IKeySet<Event>> GetEventKeys(string topic, CancellationToken token)
        => poisonEventQueue.GetKeys(topic, token);

    public Task Add(Event @event, string reason, CancellationToken token)
    {
        var topicPartitionOffset = @event.GetTopicPartitionOffset();
        var rawEvent = _deadMessageConsumer.Value.Consume(topicPartitionOffset, token);
        return poisonEventQueue.Enqueue(rawEvent, DateTime.UtcNow, reason, token);
    }

    public void Dispose()
    {
        if (_deadMessageConsumer.IsValueCreated)
            _deadMessageConsumer.Value.Close();
    }

    private sealed class ThreadSafeConsumer
    {
        private readonly IConsumer<Guid, ConsumedMessage> _deadMessageConsumer;
        private readonly object _lockObject = new();

        public ThreadSafeConsumer(
            KafkaConsumerSettings sourceSettings,
            string topic,
            ILogger logger)
        {
            if (string.IsNullOrEmpty(sourceSettings.Config.GroupId))
                throw new InvalidOperationException("Group Id is not specified.");

            var settings = sourceSettings.Duplicate();
            settings.Config.EnableAutoCommit = false;
            settings.Config.EnableAutoOffsetStore = false;
            settings.Config.AutoOffsetReset = AutoOffsetReset.Error;
            settings.Config.AllowAutoCreateTopics = false;
            settings.Config.GroupId += "_dlq";

            _deadMessageConsumer = settings.CreateBuilder()
                .SetKeyDeserializer(KeyGuidDeserializer.Instance)
                .SetValueDeserializer(ByteArrayWrapperDeserializer.Instance)
                .SetErrorHandler((a, e) => logger.LogError(
                    "{ErrorSource} internal error: Topic={Topic}, Reason={Reason}, " +
                    "Fatal={IsFatal}, IsLocal= {IsLocalError}, IsBroker={IsBrokerError}",
                    nameof(PoisonEventInbox), topic, e.Reason,
                    e.IsFatal, e.IsLocalError, e.IsBrokerError))
                .Build();
        }

        public void Close()
        {
            lock (_lockObject)
            {
                _deadMessageConsumer.Close();
                _deadMessageConsumer.Dispose();
            }
        }

        public ConsumeResult<byte[], byte[]> Consume(
            TopicPartitionOffset topicPartitionOffset,
            CancellationToken cancellationToken)
        {
            lock (_lockObject)
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

                    //TODO: Ask why? just return original one
                    return new ConsumeResult<byte[], byte[]>
                    {
                        Message = new Message<byte[], byte[]>
                        {
                            Key = rawEvent.Message.Key.ToByteArray(), // very very dangerous and depends on guid deserialization
                            Value = (byte[]?)rawEvent.Message.Value.Message ?? [],
                            Headers = rawEvent.Message.Headers,
                            Timestamp = rawEvent.Message.Timestamp
                        },
                        TopicPartitionOffset = rawEvent.TopicPartitionOffset,
                        IsPartitionEOF = rawEvent.IsPartitionEOF
                    };
                }
                finally
                {
                    _deadMessageConsumer.Unassign();
                }
            }
        }

        public sealed class ByteArrayWrapperDeserializer : IDeserializer<ConsumedMessage>
        {
            internal static readonly IDeserializer<ConsumedMessage> Instance = new ByteArrayWrapperDeserializer(); 
        
            public ConsumedMessage Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
            {
                return new ConsumedMessage(
                    !isNull ? data.ToArray() : null,
                    DeserializationStatus.Success,
                    Array.Empty<KeyValuePair<string, object>>());
            }
        }
    }
}