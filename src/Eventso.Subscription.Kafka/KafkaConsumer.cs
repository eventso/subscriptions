using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Eventso.Subscription.Kafka.DeadLetter;
using Microsoft.Extensions.Logging;

namespace Eventso.Subscription.Kafka
{
    public sealed class KafkaConsumer : IDisposable
    {
        private readonly IObserverFactory _observerFactory;
        private readonly IPoisonRecordInbox _poisonRecordInbox;
        private readonly ConsumerSettings _settings;
        private readonly int _maxObserveInterval;
        private readonly ILogger<KafkaConsumer> _logger;
        private readonly IConsumer<Guid, ConsumedMessage> _consumer;

        public KafkaConsumer(
            IObserverFactory observerFactory,
            IDeserializer<ConsumedMessage> deserializer,
            IPoisonRecordInbox poisonRecordInbox,
            ConsumerSettings settings,
            ILogger<KafkaConsumer> logger)
        {
            if (deserializer == null) throw new ArgumentNullException(nameof(deserializer));

            _observerFactory = observerFactory ?? throw new ArgumentNullException(nameof(observerFactory));
            _poisonRecordInbox = poisonRecordInbox; // can be null in case of disabled DLQ
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
                var result = await Consume(tokenSource.Token);

                timeoutTokenSource.CancelAfter(_maxObserveInterval);

                try
                {
                    await Observe(result, observer, tokenSource.Token);

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

            async Task<ConsumeResult<Guid, ConsumedMessage>> Consume(CancellationToken token)
            {
                try
                {
                    return _consumer.Consume(token);
                }
                catch (ConsumeException ex) when (ex.Error.Code == ErrorCode.Local_ValueDeserialization)
                {
                    await _poisonRecordInbox?.Add(
                        ex.ConsumerRecord,
                        $"Deserialization failed: {ex.Message}.",
                        token);

                    _logger.LogError(ex, "Serialization exception for message:" + ex.ConsumerRecord?.TopicPartitionOffset);
                    throw;
                }
            }
        }

        private async Task Observe(
            ConsumeResult<Guid, ConsumedMessage> result,
            IObserver<Event> observer,
            CancellationToken token)
        {
            var @event = new Event(result, _settings.Topic);

            try
            {
                await observer.OnEventAppeared(@event, token);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                throw new EventHandlingException(
                    result.TopicPartitionOffset.ToString(),
                    "Event observing failed",
                    ex);
            }
        }
    }
}