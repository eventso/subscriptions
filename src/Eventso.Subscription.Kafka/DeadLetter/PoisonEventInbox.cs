using System.Runtime.InteropServices;
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
    private readonly ThreadSafeConsumer _deadMessageConsumer = new(settings, topic, logger);

    public ValueTask<bool> IsPartOfPoisonStream(Event @event, CancellationToken token)
    {
        return poisonEventQueue.Contains(
            @event.GetTopicPartitionOffset().TopicPartition,
            @event.GetKey(),
            token);
    }

    public Task Add(Event @event, string reason, CancellationToken token)
    {
        var topicPartitionOffset = @event.GetTopicPartitionOffset();
        var rawEvent = _deadMessageConsumer.Consume(topicPartitionOffset, token);
        return poisonEventQueue.Enqueue(rawEvent, DateTime.UtcNow, reason, token);
    }

    public void Dispose()
        => _deadMessageConsumer.Close();

    private sealed class ThreadSafeConsumer
    {
        private readonly IConsumer<Guid, ConsumedMessage> _deadMessageConsumer;
        private readonly object _lockObject = new();

        public ThreadSafeConsumer(
            KafkaConsumerSettings settings,
            string topic,
            ILogger logger)
        {
            if (string.IsNullOrWhiteSpace(settings.Config.BootstrapServers))
                throw new InvalidOperationException("Brokers are not specified.");

            if (string.IsNullOrEmpty(settings.Config.GroupId))
                throw new InvalidOperationException("Group Id is not specified.");

            var config = new ConsumerConfig(settings.Config.ToDictionary(e => e.Key, e => e.Value))
            {
                EnableAutoCommit = false,
                EnableAutoOffsetStore = false,
                AutoOffsetReset = AutoOffsetReset.Error,
                AllowAutoCreateTopics = false
            };

            _deadMessageConsumer = settings.CreateBuilder()
                .SetKeyDeserializer(KeyGuidDeserializer.Instance)
                .SetValueDeserializer(ByteArrayWrapperDeserializer.Instance)
                .SetErrorHandler((_, e) => logger.LogError(
                    $"{nameof(PoisonEventInbox)} internal error: Topic: {topic}, {e.Reason}," +
                    $" Fatal={e.IsFatal}, IsLocal= {e.IsLocalError}, IsBroker={e.IsBrokerError}"))
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