using Eventso.Subscription.Kafka;

namespace Eventso.Subscription.Hosting;

public sealed class SubscriptionHost : BackgroundService
{
    private readonly IReadOnlyCollection<SubscriptionConfiguration> _subscriptions;
    private readonly IMessagePipelineFactory _messagePipelineFactory;
    private readonly IMessageHandlersRegistry _handlersRegistry;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger _logger;

    public SubscriptionHost(
        IEnumerable<ISubscriptionCollection> subscriptions,
        IMessagePipelineFactory messagePipelineFactory,
        IMessageHandlersRegistry handlersRegistry,
        ILoggerFactory loggerFactory)
    {
        _subscriptions = (subscriptions ?? throw new ArgumentNullException(nameof(subscriptions)))
            .SelectMany(x => x)
            .ToArray();

        _messagePipelineFactory =
            messagePipelineFactory ?? throw new ArgumentNullException(nameof(messagePipelineFactory));
        _handlersRegistry = handlersRegistry;
        _loggerFactory = loggerFactory;
        _logger = loggerFactory.CreateLogger<SubscriptionHost>();
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
            _logger.LogInformation(
                $"Subscription starting. Topics {topics}. Group {config.Settings.Config.GroupId}");

            try
            {
                using var consumer = CreateConsumer(config);
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
            }
        }
    }

    private KafkaConsumer CreateConsumer(SubscriptionConfiguration config)
    {
        return new KafkaConsumer(
            config.GetTopics(),
            new ObserverFactory(
                config,
                _messagePipelineFactory,
                _handlersRegistry,
                _loggerFactory),
            new ValueDeserializer(
                new CompositeDeserializer(config.TopicConfigurations.Select(c => (c.Topic, c.Serializer))),
                _handlersRegistry),
            // TODO get some service from DI instead of default
            //config.EnableDeadLetterQueue ? default : null,
            default,
            config.Settings.Config,
            _loggerFactory.CreateLogger<KafkaConsumer>());
    }
}