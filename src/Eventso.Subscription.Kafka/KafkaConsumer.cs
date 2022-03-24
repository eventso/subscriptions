using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Eventso.Subscription.Kafka
{
    public sealed class KafkaConsumer : IDisposable
    {
        private static readonly IDeserializer<Guid> KeyDeserializer = new KeyGuidDeserializer();

        private readonly IObserverFactory _observerFactory;
        private readonly ConsumerSettings _settings;
        private readonly int _maxObserveInterval;
        private readonly ILogger<KafkaConsumer> _logger;
        private readonly IConsumer<Guid, ConsumedMessage> _consumer;

        public KafkaConsumer(
            IObserverFactory observerFactory,
            IDeserializer<ConsumedMessage> deserializer,
            ConsumerSettings settings,
            ILogger<KafkaConsumer> logger)
        {
            if (deserializer == null) throw new ArgumentNullException(nameof(deserializer));

            _observerFactory = observerFactory ?? throw new ArgumentNullException(nameof(observerFactory));
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
                .SetKeyDeserializer(KeyDeserializer)
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
                var result = Consume();

                timeoutTokenSource.CancelAfter(_maxObserveInterval);

                try
                {
                    if (result != null)
                        await Observe(result, observer, tokenSource.Token);
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

            ConsumeResult<Guid, ConsumedMessage> Consume()
            {
                try
                {
                    return _consumer.Consume(_settings.ConsumeTimeout);
                }
                catch (ConsumeException ex) when (ex.Error.Code == ErrorCode.Local_ValueDeserialization)
                {
                    _logger.LogError(ex, "Serialization exception for message:" + ex.ConsumerRecord?.TopicPartitionOffset);
                    throw;
                }
            }
        }

        private async Task Observe(
            ConsumeResult<Guid, ConsumedMessage> result,
            IObserver<Message> observer,
            CancellationToken token)
        {
            var message = new Message(result, _settings.Topic);

            try
            {
                await observer.OnMessageAppeared(message, token);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                throw new MessageHandlingException(
                    result.TopicPartitionOffset.ToString(),
                    "Message observing failed",
                    ex);
            }
        }

        private async Task ObserveTimeout(IObserver<Message> observer, CancellationToken token)
        {
            try
            {
                await observer.OnMessageTimeout(token);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                throw new MessageHandlingException(
                    _settings.Topic,
                    "OnMessageTimeout failed",
                    ex);
            }
        }
    }
}