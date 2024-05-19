using System.Collections.Frozen;
using System.Diagnostics;
using Confluent.Kafka;
using Eventso.Subscription.Kafka.DeadLetter;
using Microsoft.Extensions.Logging;

namespace Eventso.Subscription.Kafka;

public sealed class KafkaConsumer : ISubscriptionConsumer
{
    private readonly FrozenDictionary<string, string> _topics;
    private readonly IObserverFactory _observerFactory;
    private readonly IPoisonRecordInbox _poisonRecordInbox;
    private readonly int _maxObserveInterval;
    private readonly ILogger<KafkaConsumer> _logger;
    private readonly IConsumer<Guid, ConsumedMessage> _consumer;
    private readonly bool _autoCommitMode;
    private readonly TimeSpan _observeMaxDelay;
    private readonly Dictionary<string, Task> _pausedTopicsObservers = new(1);
    private readonly List<TopicPartition> _pausedTopicPartitions = new();

    public KafkaConsumer(
        string[] topics,
        IObserverFactory observerFactory,
        IDeserializer<ConsumedMessage> deserializer,
        IPoisonRecordInbox poisonRecordInbox,
        KafkaConsumerSettings settings,
        ILogger<KafkaConsumer> logger)
    {
        if (deserializer == null) throw new ArgumentNullException(nameof(deserializer));

        var topicsSettings = topics.ToFrozenDictionary(x => x);

        if (topicsSettings.Count == 0 || topicsSettings.Keys.Any(string.IsNullOrWhiteSpace))
            throw new InvalidOperationException("Topics are not specified or contains empty value.");

        _topics = topicsSettings;

        _observerFactory = observerFactory ?? throw new ArgumentNullException(nameof(observerFactory));
        _poisonRecordInbox = poisonRecordInbox; // can be null in case of disabled DLQ

        _maxObserveInterval = (settings.Config.MaxPollIntervalMs ?? 300000) + 500;
        _observeMaxDelay = settings.PauseAfterObserveDelay
            ?? TimeSpan.FromMilliseconds(settings.Config.SessionTimeoutMs ?? 45000);

        _logger = logger;

        _consumer = settings.CreateBuilder()
            .SetKeyDeserializer(KeyGuidDeserializer.Instance)
            .SetValueDeserializer(deserializer)
            .SetErrorHandler((_, e) => _logger.LogError(
                $"KafkaConsumer internal error: Topics: {string.Join(",", _topics.Keys)}, {e.Reason}, Fatal={e.IsFatal}," +
                $" IsLocal= {e.IsLocalError}, IsBroker={e.IsBrokerError}"))
            .Build();

        _autoCommitMode = settings.Config.EnableAutoCommit == true;

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
        using var timeoutTokenSource = new CancellationTokenSource();
        using var tokenSource = CancellationTokenSource.CreateLinkedTokenSource(
            stoppingToken,
            timeoutTokenSource.Token);

        var consumer = new ConsumerAdapter(tokenSource, _consumer, _autoCommitMode);

        using var observers = new ObserverCollection(
            _topics.Keys.Select(t => (t, _observerFactory.Create(consumer, t))).ToArray());

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

                if (!string.IsNullOrEmpty(result.Topic))
                    result.Topic = _topics[result.Topic]; // topic name interning 

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

                    await WaitPendingPause(topic);

                    PauseUntil(DelayedThrow(), topic);

                    return null;

                    async Task DelayedThrow()
                    {
                        await Task.Delay(_maxObserveInterval * 2, token);

                        throw new ConsumeException(ex.ConsumerRecord, ex.Error, ex.InnerException);
                    }
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
        await WaitPendingPause(result.Topic);

        var observer = observers.GetObserver(result.Topic);

        var observeTask = Observe(result, observer, token);

        try
        {
            await observeTask.WaitAsync(_observeMaxDelay, cancellationToken: default);
        }
        catch (TimeoutException)
        {
            PauseUntil(observeTask, result.Topic);
        }
    }

    private void PauseUntil(Task waitingTask, string topic)
    {
        PauseAssignments(topic);

        var resumeTask = ResumeOnComplete(waitingTask, topic);
        _pausedTopicsObservers.Add(topic, resumeTask);
    }

    private Task WaitPendingPause(string topic)
    {
        if (_pausedTopicsObservers.Count > 0 &&
            _pausedTopicsObservers.Remove(topic, out var pausedTask))
        {
            _logger.LogInformation("Waiting paused task for topic {Topic}", topic);
            return pausedTask;
        }

        return Task.CompletedTask;
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
            : _consumer.Assignment.Where(t => t.Topic.Equals(topic)).ToList();

        _consumer.Pause(assignments);

        lock (_pausedTopicPartitions)
            _pausedTopicPartitions.AddRange(assignments);

        _logger.LogInformation($"Topic '{topic}' consuming paused. Partitions: " +
            string.Join(',', assignments.Select(x => x.Partition.Value)));
    }

    private void ResumeAssignments(string topic)
    {
        List<TopicPartition> resumed;

        lock (_pausedTopicPartitions)
        {
            resumed = _pausedTopicPartitions.FindAll(t => t.Topic.Equals(topic));
            _pausedTopicPartitions.RemoveAll(t => t.Topic.Equals(topic));
        }

        _consumer.Resume(resumed);

        _logger.LogInformation($"Topic '{topic}' consuming resumed. Partitions: " +
            string.Join(',', resumed.Select(x => x.Partition.Value)));
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
            => _items = items;

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
                observer.Dispose();
        }
    }
}