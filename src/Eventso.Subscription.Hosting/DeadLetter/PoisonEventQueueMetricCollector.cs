using System.Collections.Frozen;
using System.Diagnostics.Metrics;
using Eventso.Subscription.Kafka.DeadLetter;

namespace Eventso.Subscription.Hosting.DeadLetter;

public sealed class PoisonEventQueueMetricCollector : BackgroundService
{
    private static readonly TimeSpan PollInterval = TimeSpan.FromMinutes(1);

    private readonly IPoisonEventStore _poisonEventStore;
    private readonly ILogger<PoisonEventQueueMetricCollector> _logger;

    private readonly FrozenDictionary<ConsumingTarget, PoisonCounter> _measurements;
    private readonly object _lockObject = new();

    private bool _isInitialized = false;

    public PoisonEventQueueMetricCollector(
        IEnumerable<ISubscriptionCollection> subscriptions,
        IPoisonEventStore poisonEventStore,
        ILogger<PoisonEventQueueMetricCollector> logger)
    {
        _measurements = subscriptions
            .SelectMany(s =>
                s.SelectMany(ss =>
                    ss.TopicConfigurations.Select(sss =>
                        new ConsumingTarget(sss.Topic, ss.Settings.Config.GroupId))))
            .ToFrozenDictionary(s => s, s => new PoisonCounter(s));

        _poisonEventStore = poisonEventStore;
        _logger = logger;

        Diagnostic.Meter.CreateObservableGauge("dlq.size", CollectMeasurements);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(PollInterval, stoppingToken);
                await UpdateMeasurements(stoppingToken);
            }
            catch (Exception e)
            {
                _logger.LogWarning(e, "Exception occured while sending DLQ metrics");
            }
        }
    }

    private async Task UpdateMeasurements(CancellationToken stoppingToken)
    {
        var poisonCounters = await _poisonEventStore.CountPoisonedEvents(stoppingToken);

        foreach (var (consumingTarget, measurementCounter) in _measurements)
            measurementCounter.Value = poisonCounters.GetValueOrDefault(consumingTarget);

        _isInitialized = true;
    }

    private Measurement<long>[] CollectMeasurements()
        => _isInitialized ? _measurements.Select(v => v.Value.ToMeasurement()).ToArray() : [];

    private sealed class PoisonCounter(ConsumingTarget consumingTarget)
    {
        private readonly KeyValuePair<string, object?>[] _tags = new[]
        {
            new KeyValuePair<string, object?>("topic", consumingTarget.Topic),
            new KeyValuePair<string, object?>("group", consumingTarget.GroupId)
        };

        public long Value { get; set; }

        public Measurement<long> ToMeasurement()
            => new(Value, _tags);
    }
}