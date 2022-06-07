using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Eventso.Subscription.Observing.DeadLetter;
using Microsoft.Extensions.Logging;

namespace Eventso.Subscription.Kafka
{
    public sealed class KafkaConsumer : IDisposable
    {
        private readonly IObserverFactory<Event> _observerFactory;
        private readonly IPoisonEventInbox<Event> _poisonEventInbox;
        private readonly ConsumerSettings _settings;
        private readonly int _maxObserveInterval;
        private readonly ILogger<KafkaConsumer> _logger;
        private readonly IConsumer<Guid, ConsumedMessage> _consumer;

        public KafkaConsumer(
            IObserverFactory<Event> observerFactory,
            IDeserializer<ConsumedMessage> deserializer,
            IPoisonEventInbox<Event> poisonEventInbox,
            ConsumerSettings settings,
            ILogger<KafkaConsumer> logger)
        {
            if (deserializer == null) throw new ArgumentNullException(nameof(deserializer));

            _observerFactory = observerFactory ?? throw new ArgumentNullException(nameof(observerFactory));
            _poisonEventInbox = poisonEventInbox; // can be null in case of disabled DLQ
            _settings = settings;
            _maxObserveInterval = (settings.Config.MaxPollIntervalMs ?? 300000) + 500;
            _logger = logger;


            if (string.IsNullOrWhiteSpace(settings.Config.BootstrapServers))
                throw new InvalidOperationException("Brokers are not specified.");

            if (string.IsNullOrEmpty(settings.Topic))
                throw new InvalidOperationException("Topics are not specified.");

            if (string.IsNullOrEmpty(settings.Config.GroupId))
                throw new InvalidOperationException("Group Id is not specified.");

            _consumer = new ConsumerBuilder<Guid, ConsumedMessage>(settings.Config)
                .SetKeyDeserializer(KeyGuidDeserializer.Instance)
                .SetValueDeserializer(deserializer)
                .SetErrorHandler((_, e) => _logger.LogError(
                    $"KafkaConsumer internal error: Topic: {settings.Topic}, {e.Reason}, Fatal={e.IsFatal}," +
                    $" IsLocal= {e.IsLocalError}, IsBroker={e.IsBrokerError}"))
                .Build();

            _consumer.Subscribe(settings.Topic);
        }

        public void Close() => _consumer.Close();

        public void Dispose() => _consumer.Dispose();

        public async Task Consume(CancellationToken stoppingToken)
        {
            await Task.Yield();

            using var timeoutTokenSource = new CancellationTokenSource();
            using var tokenSource = CancellationTokenSource.CreateLinkedTokenSource(
                stoppingToken,
                timeoutTokenSource.Token);

            var consumer = new ConsumerAdapter(tokenSource, _consumer);
            var observer = _observerFactory.Create(consumer);

            using var disposableObserver = observer as IDisposable;

            while (!tokenSource.IsCancellationRequested)
            {
                var @event = await Consume();

                timeoutTokenSource.CancelAfter(_maxObserveInterval);

                try
                {
                    if (@event != null)
                        await Observe(@event.Value, observer, tokenSource.Token);
                    else
                        await ObserveTimeout(observer, tokenSource.Token);

                    tokenSource.Token.ThrowIfCancellationRequested();
                }
                catch (OperationCanceledException)
                {
                    if (timeoutTokenSource.IsCancellationRequested)
                        throw new TimeoutException(
                            $"Observing time is out. Timeout: {_maxObserveInterval}ms. " +
                            "Consider to increase MaxPollInterval.");

                    throw;
                }

                timeoutTokenSource.CancelAfter(Timeout.Infinite);
            }

            async Task<Event?> Consume()
            {
                try
                {
                    var result = _consumer.Consume(_settings.ConsumeTimeout);
                    return result != null
                        ? new Event(result, _settings.Topic)
                        : null;
                }
                catch (ConsumeException ex) when (ex.Error.Code == ErrorCode.Local_ValueDeserialization)
                {
                    if (_poisonEventInbox != null)
                    {
                        var @event = new Event(
                            new ConsumeResult<Guid, ConsumedMessage>()
                            {
                                TopicPartitionOffset = ex.ConsumerRecord.TopicPartitionOffset,
                                Message = new Message<Guid, ConsumedMessage>()
                                {
                                    Key = KeyGuidDeserializer.Instance.Deserialize(ex.ConsumerRecord.Message.Key,
                                        ex.ConsumerRecord.Message.Key.Length == 0, SerializationContext.Empty),
                                    Value = ConsumedMessage.Skipped,
                                    Timestamp = ex.ConsumerRecord.Message.Timestamp,
                                    Headers = ex.ConsumerRecord.Message.Headers

                                },
                                IsPartitionEOF = ex.ConsumerRecord.IsPartitionEOF
                            },
                            _settings.Topic);

                        // this approach will force inbox to reconsume this message, but it is a really rare case
                        await _poisonEventInbox.Add(
                            new PoisonEvent<Event>(@event, $"Deserialization failed: {ex.Message}."),
                            tokenSource.Token);

                        return @event;
                    }

                    _logger.LogError(ex, "Serialization exception for message:" + ex.ConsumerRecord?.TopicPartitionOffset);
                    throw;
                }
            }
        }

        private async Task Observe(
            Event @event,
            IObserver<Event> observer,
            CancellationToken token)
        {
            try
            {
                await observer.OnEventAppeared(@event, token);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                throw new EventHandlingException(
                    @event.GetTopicPartitionOffset().ToString(),
                    "Event observing failed",
                    ex);
            }
        }

        private async Task ObserveTimeout(IObserver<Event> observer, CancellationToken token)
        {
            try
            {
                await observer.OnEventTimeout(token);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                throw new EventHandlingException(
                    _settings.Topic,
                    "OnEventTimeout failed",
                    ex);
            }
        }
    }
}