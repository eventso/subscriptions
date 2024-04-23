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
        return poisonEventQueue.IsPoison(
            @event.GetTopicPartitionOffset().TopicPartition,
            @event.GetKey(),
            token);
    }

    public Task Add(Event @event, string reason, CancellationToken token)
    {
        var topicPartitionOffset = @event.GetTopicPartitionOffset();
        var rawEvent = _deadMessageConsumer.Consume(topicPartitionOffset, token);
        var openingPoisonEvent = PoisonEvent.From(rawEvent);
        return poisonEventQueue.Blame(openingPoisonEvent, DateTime.UtcNow, reason, token);
    }

    public void Dispose()
        => _deadMessageConsumer.Close();

    private sealed class ThreadSafeConsumer
    {
        private readonly IConsumer<byte[], byte[]> _deadMessageConsumer;
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

            _deadMessageConsumer = new ConsumerBuilder<byte[], byte[]>(config)
                .SetKeyDeserializer(Deserializers.ByteArray)
                .SetValueDeserializer(Deserializers.ByteArray)
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

                    return rawEvent;
                }
                finally
                {
                    _deadMessageConsumer.Unassign();
                }
            }
        }
    }
}