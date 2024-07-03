using System.Collections.Concurrent;
using Confluent.Kafka;
using Eventso.Subscription.Kafka;

namespace Eventso.Subscription.Hosting;

public sealed class SubscriptionHost : BackgroundService, ISubscriptionHost
{
    private static readonly TimeSpan CriticalErrorDelay = TimeSpan.FromSeconds(30); 
    
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
                c.ClonePerConsumerInstance().Select(instanceConfig => RunConsuming(instanceConfig, stoppingToken)))
            .ToArray();

        return Task.WhenAll(subscriptionTasks);
    }

    private async Task RunConsuming(SubscriptionConfiguration config, CancellationToken cancellationToken)
    {
        var topics = string.Join(',', config.GetTopics());

        await Task.Yield();

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
            catch (ConsumeException consumeException)
            {
                _logger.LogError(consumeException, $"Consumer failed. Topic: {topics}. Pausing for {CriticalErrorDelay}");
                activity?.SetException(consumeException);
                
                if (consumeException.Error.Code is
                    ErrorCode.InconsistentGroupProtocol or ErrorCode.InvalidGroupId
                    or ErrorCode.BrokerNotAvailable
                    or ErrorCode.InvalidPartitions or ErrorCode.UnknownTopicOrPart or ErrorCode.TopicException
                    or ErrorCode.TopicAuthorizationFailed
                    or ErrorCode.ClusterAuthorizationFailed or ErrorCode.GroupAuthorizationFailed)
                {
                    await Task.Delay(CriticalErrorDelay, cancellationToken);
                }
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