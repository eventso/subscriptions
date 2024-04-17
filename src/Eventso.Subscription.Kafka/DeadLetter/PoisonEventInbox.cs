using System.Runtime.InteropServices;
using Confluent.Kafka;
using Eventso.Subscription.Kafka.DeadLetter.Store;
using Eventso.Subscription.Observing.DeadLetter;
using Microsoft.Extensions.Logging;

namespace Eventso.Subscription.Kafka.DeadLetter;

public sealed class PoisonEventInbox : IPoisonEventInbox<Event>, IDisposable
{
    private readonly IPoisonEventManager _poisonEventManager;
    private readonly ThreadSafeConsumer _deadMessageConsumer;

    public PoisonEventInbox(
        IPoisonEventManager poisonEventManager,
        KafkaConsumerSettings settings,
        string[] topics,
        ILogger<PoisonEventInbox> logger)
    {
        _poisonEventManager = poisonEventManager;
        _deadMessageConsumer = new ThreadSafeConsumer(settings, topics, logger);
    }

    public ValueTask<bool> IsPartOfPoisonStream(Event @event, CancellationToken token)
    {
        return _poisonEventManager.IsPoison(
            @event.GetTopicPartitionOffset().TopicPartition,
            @event.GetKey(),
            token);
    }

    public Task Add(Event @event, string reason, CancellationToken token)
    {
        var topicPartitionOffset = @event.GetTopicPartitionOffset();
        var rawEvent = _deadMessageConsumer.Consume(topicPartitionOffset, token);
        var openingPoisonEvent = PoisonEvent.From(rawEvent);
        return _poisonEventManager.Blame(openingPoisonEvent, DateTime.UtcNow, reason, token);
    }

    public void Dispose()
        => _deadMessageConsumer.Close();

    private sealed class ThreadSafeConsumer
    {
        private readonly IConsumer<byte[], byte[]> _deadMessageConsumer;
        private readonly object _lockObject = new();

        public ThreadSafeConsumer(
            KafkaConsumerSettings settings,
            string[] topics,
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
                AllowAutoCreateTopics = false,
                GroupId = settings.Config.GroupId + "_cemetery" // boo!
            };

            _deadMessageConsumer = new ConsumerBuilder<byte[], byte[]>(config)
                .SetKeyDeserializer(Deserializers.ByteArray)
                .SetValueDeserializer(Deserializers.ByteArray)
                .SetErrorHandler((_, e) => logger.LogError(
                    $"{nameof(PoisonEventInbox)} internal error: Topics: {string.Join(", ", topics)}, {e.Reason}," +
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