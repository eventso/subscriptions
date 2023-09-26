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
    private readonly bool _autoCommitMode;
    private readonly TimeSpan _observeTimeout;
    private readonly Dictionary<string, Task> _pausedTopicsObservers = new(1);

    public KafkaConsumer(
        string[] topics,
        IObserverFactory observerFactory,
        IDeserializer<ConsumedMessage> deserializer,
        IPoisonRecordInbox poisonRecordInbox,
        ConsumerConfig config,
        ILogger<KafkaConsumer> logger)
        : this(
            topics,
            observerFactory,
            deserializer,
            poisonRecordInbox,
            new ConsumerBuilder<Guid, ConsumedMessage>(config),
            logger,
            config.MaxPollIntervalMs,
            config.SessionTimeoutMs,
            config.EnableAutoCommit)
    {
        if (string.IsNullOrWhiteSpace(config.BootstrapServers))
            throw new InvalidOperationException("Brokers are not specified.");

        if (string.IsNullOrEmpty(config.GroupId))
            throw new InvalidOperationException("Group Id is not specified.");
    }

    public KafkaConsumer(
        string[] topics,
        IObserverFactory observerFactory,
        IDeserializer<ConsumedMessage> deserializer,
        IPoisonRecordInbox poisonRecordInbox,
        ConsumerBuilder<Guid, ConsumedMessage> consumerBuilder,
        ILogger<KafkaConsumer> logger,
        int? maxPollIntervalMs,
        int? sessionTimeoutMs,
        bool? enableAutoCommit)
    {
        if (deserializer == null) throw new ArgumentNullException(nameof(deserializer));

        if (topics.Length == 0 || !Array.TrueForAll(topics, t => !string.IsNullOrEmpty(t)))
            throw new InvalidOperationException("Topics are not specified or contains empty value.");

        _topics = new TopicDictionary<string>(topics.Select(t => (t, t)));

        _observerFactory = observerFactory ?? throw new ArgumentNullException(nameof(observerFactory));
        _poisonRecordInbox = poisonRecordInbox; // can be null in case of disabled DLQ
        _maxObserveInterval = (maxPollIntervalMs ?? 300000) + 500;
        _observeTimeout = TimeSpan.FromMilliseconds(sessionTimeoutMs ?? 45000);
        _logger = logger;

        _consumer = consumerBuilder
            .SetKeyDeserializer(KeyGuidDeserializer.Instance)
            .SetValueDeserializer(deserializer)
            .SetErrorHandler((_, e) => _logger.LogError(
                $"KafkaConsumer internal error: Topics: {string.Join(",", _topics.Items)}, {e.Reason}, Fatal={e.IsFatal}," +
                $" IsLocal= {e.IsLocalError}, IsBroker={e.IsBrokerError}"))
            .Build();

        _autoCommitMode = enableAutoCommit == true;

        _consumer.Subscribe(topics);
    }

    public void Close()
    {
        if (_autoCommitMode)
        {
            try
            {
                _consumer.Commit();
            }
            catch
            {
            }
        }

        _consumer.Close();
    }

    public void Dispose() => _consumer.Dispose();

    public async Task Consume(CancellationToken stoppingToken)
    {
        await Task.Yield();

        using var timeoutTokenSource = new CancellationTokenSource();
        using var tokenSource = CancellationTokenSource.CreateLinkedTokenSource(
            stoppingToken,
            timeoutTokenSource.Token);

        var consumer = new ConsumerAdapter(tokenSource, _consumer, _autoCommitMode);

        using var observers = new ObserverCollection(
            _topics.Items.Select(t => (t, _observerFactory.Create(consumer, t))).ToArray());

        while (!tokenSource.IsCancellationRequested)
        {
            using var tracingScope = Diagnostic.StartRooted(KafkaDiagnostic.Consume);

            var result = await ConsumeOnce(tracingScope.Activity, tokenSource.Token);

            if (result == null)
                continue;

            try
            {
                var handleTask = HandleResult(observers, result, tokenSource.Token);

                var handleCompletedSynchronously = handleTask.IsCompleted;

                if (!handleCompletedSynchronously)
                    timeoutTokenSource.CancelAfter(_maxObserveInterval);

                await handleTask;

                tokenSource.Token.ThrowIfCancellationRequested();

                if (!handleCompletedSynchronously)
                    timeoutTokenSource.CancelAfter(Timeout.Infinite);
            }
            catch (OperationCanceledException)
            {
                if (timeoutTokenSource.IsCancellationRequested)
                    throw new TimeoutException(
                        $"Observing time is out. Topic: {result.Topic}, timeout: {_maxObserveInterval}ms. " +
                        "Consider to increase MaxPollInterval.");

                tokenSource.Cancel();
                throw;
            }
            catch (Exception exception)
            {
                tokenSource.Cancel();
                tracingScope.Activity?.SetException(exception);
                throw;
            }
        }

        async ValueTask<ConsumeResult<Guid, ConsumedMessage>?> ConsumeOnce(Activity? activity, CancellationToken token)
        {
            try
            {
                var result = _consumer.Consume(token);

                //TODO:https://github.com/CommunityToolkit/dotnet/blob/main/src/CommunityToolkit.HighPerformance/Buffers/StringPool.cs
                if (!string.IsNullOrEmpty(result.Topic))
                    result.Topic = _topics.Get(result.Topic); // topic name interning 

                activity?.SetTags(result);

                return result;
            }
            catch (ConsumeException ex) when (ex.Error.Code == ErrorCode.Local_ValueDeserialization)
            {
                //await _poisonRecordInbox?.Add(
                //    ex.ConsumerRecord,
                //    $"Deserialization failed: {ex.Message}.",
                //    token);
                activity?.SetException(ex);

                _logger.LogError(ex,
                    $"Serialization exception for message {ex.ConsumerRecord?.TopicPartitionOffset}, consuming paused");

                if (ex.ConsumerRecord != null)
                {
                    activity?.SetTags(ex.ConsumerRecord);
                    var topic = ex.ConsumerRecord.Topic;

                    if (_pausedTopicsObservers.Remove(topic, out var task))
                        await task;

                    PauseUntil(Task.Delay(_maxObserveInterval, token), topic);

                    return null;
                }

                throw;
            }
        }
    }

    private async Task HandleResult(
        ObserverCollection observers,
        ConsumeResult<Guid, ConsumedMessage> result,
        CancellationToken token)
    {
        if (_pausedTopicsObservers.Count > 0 &&
            _pausedTopicsObservers.Remove(result.Topic, out var pausedTask))
        {
            _logger.LogInformation($"Waiting paused task for topic {result.Topic}");
            await pausedTask;
        }

        var observer = observers.GetObserver(result.Topic);

        var observeTask = Observe(result, observer, token);

        if (!observeTask.IsCompleted)
        {
            await Task.WhenAny(observeTask, Task.Delay(_observeTimeout, token));

            if (!observeTask.IsCompleted)
            {
                PauseUntil(observeTask, result.Topic);
                return;
            }
        }

        await observeTask;
    }

    private void PauseUntil(Task waitingTask, string topic)
    {
        PauseAssignments(topic);

        var resumeTask = ResumeOnComplete(waitingTask, topic);
        _pausedTopicsObservers.Add(topic, resumeTask);
    }

    private async Task ResumeOnComplete(Task waitingTask, string topic)
    {
        using var activity = Diagnostic.ActivitySource.StartActivity(KafkaDiagnostic.Pause)?
            .AddTag("topic", topic);

        try
        {
            await waitingTask;
        }
        finally
        {
            ResumeAssignments(topic);

            activity?.Dispose();
        }
    }

    public void Pause(string topic)
    {
        if (_consumer.Assignment.Any(a => a.Topic.Equals(topic)))
            PauseAssignments(topic);
    }

    public void Resume(string topic)
    {
        if (_consumer.Assignment.Any(a => a.Topic.Equals(topic)))
            ResumeAssignments(topic);
    }

    private void PauseAssignments(string topic)
    {
        var assignments = _consumer.Subscription.Count == 1
            ? _consumer.Assignment
            : _consumer.Assignment.Where(t => t.Topic.Equals(topic));

        _consumer.Pause(assignments);

        _logger.LogInformation($"Topic '{topic}' consuming paused");
    }

    private void ResumeAssignments(string topic)
    {
        var assignments = _consumer.Subscription.Count == 1
            ? _consumer.Assignment
            : _consumer.Assignment.Where(t => t.Topic.Equals(topic));

        _consumer.Resume(assignments);

        _logger.LogInformation($"Topic '{topic}' consuming resumed");
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
                result.TopicPartitionOffset.ToString()!,
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