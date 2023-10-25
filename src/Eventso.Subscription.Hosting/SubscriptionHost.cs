using System.Collections.Concurrent;
using Eventso.Subscription.Kafka;

namespace Eventso.Subscription.Hosting;

public sealed class SubscriptionHost : BackgroundService, ISubscriptionHost
{
    private readonly IConsumerFactory _consumerFactory;
    private readonly IReadOnlyCollection<SubscriptionConfiguration> _subscriptions;
    private readonly ILogger _logger;

    private readonly ConcurrentDictionary<ISubscriptionConsumer, SubscriptionConfiguration> _consumers =
        new(ReferenceEqualityComparer.Instance);

    public SubscriptionHost(
        IEnumerable<ISubscriptionCollection> subscriptions,
        IConsumerFactory consumerFactory,
        ILogger<SubscriptionHost> logger)
    {
        _consumerFactory = consumerFactory;
        _subscriptions = subscriptions.SelectMany(x => x).ToArray();
        _logger = logger;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var subscriptionTasks = _subscriptions
            .SelectMany(c =>
                c.ClonePerConsumerInstance().Select(_ => RunConsuming(c, stoppingToken)))
            .ToArray();

        return Task.WhenAll(subscriptionTasks);
    }

    private async Task RunConsuming(SubscriptionConfiguration config, CancellationToken cancellationToken)
    {
        var topics = string.Join(',', config.GetTopics());

        while (!cancellationToken.IsCancellationRequested)
        {
            using var activity = Diagnostic.ActivitySource.StartActivity(Diagnostic.HostConsuming)?
                .AddTag("topics", topics);

            _logger.LogInformation(
                $"Subscription starting. Topics {topics}. Group {config.Settings.Config.GroupId}");

            try
            {
                using var consumer = _consumerFactory.CreateConsumer(config);
                _consumers.TryAdd(consumer, config);
                try
                {
                    await consumer.Consume(cancellationToken);
                }
                finally
                {
                    _consumers.TryRemove(consumer, out _);
                    consumer.Close();
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation($"Subscription stopped. Topic: {topics}.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Subscription failed. Topic: {topics}.");
                activity?.SetException(ex);
            }
        }
    }

    public void Pause(string topic, string? groupId = null)
    {
        foreach (var consumer in FindAll(topic, groupId))
            consumer.Pause(topic);
    }

    public void Resume(string topic, string? groupId = null)
    {
        foreach (var consumer in FindAll(topic, groupId))
            consumer.Resume(topic);
    }

    private IEnumerable<ISubscriptionConsumer> FindAll(string topic, string? groupId = null)
    {
        foreach (var (consumer, config) in _consumers)
        {
            if (config.Contains(topic) && (groupId == null || config.Settings.Config.GroupId == groupId))
                yield return consumer;
        }
    }
}