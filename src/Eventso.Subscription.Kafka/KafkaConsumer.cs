using System.Collections.Frozen;
using System.Diagnostics;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Eventso.Subscription.Kafka;

public sealed class KafkaConsumer : ISubscriptionConsumer
{
    private readonly string[] _topics;
    private readonly IObserverFactory<Event> _observerFactory;
    private readonly IConsumerEventsObserver _eventsObserver;
    private readonly int _maxObserveInterval;
    private readonly ILogger<KafkaConsumer> _logger;
    private readonly IConsumer<Guid, ConsumedMessage> _consumer;
    private readonly bool _autoCommitMode;
    private readonly TimeSpan _observeMaxDelay;
    private readonly Dictionary<string, Task> _pausedTopicsObservers = new(1);
    private readonly List<TopicPartition> _pausedTopicPartitions = new();

    public KafkaConsumer(
        string[] topics,
        IObserverFactory<Event> observerFactory,
        IDeserializer<ConsumedMessage> deserializer,
        IConsumerEventsObserver eventsObserver,
        KafkaConsumerSettings settings,
        ILogger<KafkaConsumer> logger)
    {
        ArgumentNullException.ThrowIfNull(observerFactory, nameof(observerFactory));
        ArgumentNullException.ThrowIfNull(deserializer, nameof(deserializer));
        ArgumentNullException.ThrowIfNull(eventsObserver, nameof(eventsObserver));

        if (topics.Length == 0 || topics.Any(string.IsNullOrWhiteSpace))
            throw new InvalidOperationException("Topics are not specified or contains empty value.");

        _topics = topics;
        _observerFactory = observerFactory;
        _eventsObserver = eventsObserver;
        _logger = logger;

        _maxObserveInterval = (settings.Config.MaxPollIntervalMs ?? 300000) + 500;
        _observeMaxDelay = settings.PauseAfterObserveDelay
            ?? TimeSpan.FromMilliseconds(settings.Config.SessionTimeoutMs ?? 45000);

        _autoCommitMode = settings.Config.EnableAutoCommit == true;

        _consumer = BuildConsumer(settings, deserializer);
        _consumer.Subscribe(topics);
    }

    private IConsumer<Guid, ConsumedMessage> BuildConsumer(
        KafkaConsumerSettings settings,
        IDeserializer<ConsumedMessage> deserializer)
    {
        return settings.CreateBuilder()
            .SetKeyDeserializer(KeyGuidDeserializer.Instance)
            .SetValueDeserializer(deserializer)
            .SetPartitionsAssignedHandler((_, assigned) => _eventsObserver.OnAssign(assigned))
            .SetPartitionsRevokedHandler((_, revoked) => _eventsObserver.OnRevoke(revoked))
            .SetPartitionsLostHandler((_, lost) => _eventsObserver.OnRevoke(lost))
            .SetErrorHandler((_, e) =>
                _logger.ConsumeError(_topics, e.Reason, e.IsFatal, e.IsLocalError, e.IsBrokerError))
            .Build();
    }

    public void Close()
    {
        _eventsObserver.OnClose(_consumer.Assignment);

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

    public void Dispose()
    {
        _eventsObserver.OnClose(_consumer.Assignment);
        _consumer.Dispose();
    }

    public async Task Consume(CancellationToken stoppingToken)
    {
        using var timeoutTokenSource = new CancellationTokenSource();
        using var tokenSource = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, timeoutTokenSource.Token);

        var consumer = new ConsumerAdapter(tokenSource, _consumer, _autoCommitMode);

        using var observers = new TopicObserverCollection(_topics.Select(t => (t, _observerFactory.Create(consumer, t))));

        while (!tokenSource.IsCancellationRequested)
        {
            using var tracingScope = Diagnostic.StartRooted(KafkaDiagnostic.Consume);

            var result = await ConsumeOnce(tracingScope.Activity, tokenSource.Token);

            if (result == null)
                continue;

            var (topic, observer) = observers.GetTopicObserver(result.Topic);

            result.Topic = topic; // topic name interning

            var @event = new Event(result);

            try
            {
                var handleTask = HandleResult(observer, @event, tokenSource.Token);

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
                        $"Observing time is out. Topic: {@event.Topic}, timeout: {_maxObserveInterval}ms. " +
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

                if (string.IsNullOrEmpty(result.Topic))
                    throw new InvalidOperationException("Topic name is not set for consumer result.");

                activity?.SetTags(result);

                return result;
            }
            catch (ConsumeException ex) when (ex.Error.Code == ErrorCode.Local_ValueDeserialization)
            {
                activity?.SetException(ex);

                if (await _eventsObserver.TryHandleSerializationException(ex, token))
                    return null;
                
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

    private async Task HandleResult(IObserver<Event> observer, Event result, CancellationToken token)
    {
        await WaitPendingPause(result.Topic);

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
            _logger.WaitingPaused(topic);
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

        _logger.ConsumePaused(topic, string.Join(',', assignments.Select(x => x.Partition.Value)));
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

        _logger.ConsumeResumed(topic, string.Join(',', resumed.Select(x => x.Partition.Value)));
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
                @event.GetTopicPartitionOffset().ToString()!,
                "Event observing failed",
                ex);
        }
    }

    private sealed class TopicObserverCollection : IDisposable
    {
        private readonly FrozenDictionary<string, (string topic, IObserver<Event> observer)> _items;

        public TopicObserverCollection(IEnumerable<(string topic, IObserver<Event> observer)> items)
            => _items = items.ToFrozenDictionary(x => x.topic);

        public (string, IObserver<Event>) GetTopicObserver(string topic)
            => _items[topic];

        public void Dispose()
        {
            foreach (var observer in _items.Values.Select(x => x.observer))
                observer.Dispose();
        }
    }
}