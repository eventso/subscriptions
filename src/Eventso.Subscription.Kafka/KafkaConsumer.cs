using System.Diagnostics;
using Confluent.Kafka;
using Eventso.Subscription.Kafka.DeadLetter;
using Microsoft.Extensions.Logging;

namespace Eventso.Subscription.Kafka;

public sealed class KafkaConsumer : ISubscriptionConsumer
{
    private readonly TopicDictionary<string> _topics;
    private readonly IObserverFactory _observerFactory;
    private readonly IPoisonRecordInbox _poisonRecordInbox;
    private readonly int _maxObserveInterval;
    private readonly ILogger<KafkaConsumer> _logger;
    private readonly IConsumer<Guid, ConsumedMessage> _consumer;

    public KafkaConsumer(
        string[] topics,
        IObserverFactory observerFactory,
        IDeserializer<ConsumedMessage> deserializer,
        IPoisonRecordInbox poisonRecordInbox,
        ConsumerConfig config,
        ILogger<KafkaConsumer> logger)
    {
        if (deserializer == null) throw new ArgumentNullException(nameof(deserializer));

        if (string.IsNullOrWhiteSpace(config.BootstrapServers))
            throw new InvalidOperationException("Brokers are not specified.");

        if (topics.Length == 0 || !Array.TrueForAll(topics, t => !string.IsNullOrEmpty(t)))
            throw new InvalidOperationException("Topics are not specified or contains empty value.");

        if (string.IsNullOrEmpty(config.GroupId))
            throw new InvalidOperationException("Group Id is not specified.");

        _topics = new TopicDictionary<string>(topics.Select(t => (t, t)));

        _observerFactory = observerFactory ?? throw new ArgumentNullException(nameof(observerFactory));
        _poisonRecordInbox = poisonRecordInbox; // can be null in case of disabled DLQ
        _maxObserveInterval = (config.MaxPollIntervalMs ?? 300000) + 500;
        _logger = logger;

        _consumer = new ConsumerBuilder<Guid, ConsumedMessage>(config)
            .SetKeyDeserializer(KeyGuidDeserializer.Instance)
            .SetValueDeserializer(deserializer)
            .SetErrorHandler((_, e) => _logger.LogError(
                $"KafkaConsumer internal error: Topics: {string.Join(",", _topics.Items)}, {e.Reason}, Fatal={e.IsFatal}," +
                $" IsLocal= {e.IsLocalError}, IsBroker={e.IsBrokerError}"))
            .Build();

        _consumer.Subscribe(topics);
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

        using var observers = new ObserverCollection(
            _topics.Items.Select(t => (t, _observerFactory.Create(consumer, t))).ToArray());

        using var activity = Diagnostic.ActivitySource.StartActivity(KafkaDiagnostic.Consume);

        while (!tokenSource.IsCancellationRequested)
        {
            var result = await Consume(tokenSource.Token);

            timeoutTokenSource.CancelAfter(_maxObserveInterval);

            try
            {
                var observer = observers.GetObserver(result.Topic);

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
                var result = _consumer.Consume(token);

                //TODO:https://github.com/CommunityToolkit/dotnet/blob/main/src/CommunityToolkit.HighPerformance/Buffers/StringPool.cs
                if (!string.IsNullOrEmpty(result.Topic))
                    result.Topic = _topics.Get(result.Topic); // topic name interning 

                Activity.Current?.SetTags(result);

                return result;
            }
            catch (ConsumeException ex) when (ex.Error.Code == ErrorCode.Local_ValueDeserialization)
            {
                await _poisonRecordInbox?.Add(
                    ex.ConsumerRecord,
                    $"Deserialization failed: {ex.Message}.",
                    token);

                _logger.LogError(ex,
                    "Serialization exception for message:" + ex.ConsumerRecord?.TopicPartitionOffset);

                Activity.Current?.SetException(ex);

                throw;
            }
        }
    }

    private async Task Observe(
        ConsumeResult<Guid, ConsumedMessage> result,
        IObserver<Event> observer,
        CancellationToken token)
    {
        var @event = new Event(result);

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

    private sealed class ObserverCollection : IDisposable
    {
        private readonly (string topic, IObserver<Event> observer)[] _items;

        public ObserverCollection((string, IObserver<Event>)[] items)
        {
            _items = items;
        }

        public IObserver<Event> GetObserver(string topic)
        {
            if (_items.Length == 1)
                return _items[0].observer;

            for (var index = 0; index < _items.Length; index++)
            {
                ref readonly var item = ref _items[index];

                if (ReferenceEquals(item.topic, topic))
                    return item.observer;
            }

            throw new InvalidOperationException($"Observer not found for topic {topic}");
        }

        public void Dispose()
        {
            foreach (var observer in _items.Select(x => x.observer))
                if (observer is IDisposable disposable)
                    disposable.Dispose();
        }
    }
}