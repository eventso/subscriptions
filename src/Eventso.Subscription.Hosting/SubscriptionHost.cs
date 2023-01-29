namespace Eventso.Subscription.Hosting;

public sealed class SubscriptionHost : BackgroundService
{
    private readonly IConsumerFactory _consumerFactory;
    private readonly IReadOnlyCollection<SubscriptionConfiguration> _subscriptions;
    private readonly ILogger _logger;

    public SubscriptionHost(
        IEnumerable<ISubscriptionCollection> subscriptions,
        IConsumerFactory consumerFactory,
        ILogger<SubscriptionHost> logger)
    {
        _consumerFactory = consumerFactory;
        _subscriptions = (subscriptions ?? throw new ArgumentNullException(nameof(subscriptions)))
            .SelectMany(x => x)
            .ToArray();

        _logger = logger;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var subscriptionTasks = _subscriptions
            .SelectMany(c =>
                Enumerable.Range(0, c.ConsumerInstances)
                    .Select(_ => RunConsuming(c, stoppingToken)))
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
                try
                {
                    await consumer.Consume(cancellationToken);
                }
                finally
                {
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
}