using System.Collections.Frozen;
using Eventso.Subscription.Kafka;
using Eventso.Subscription.Kafka.DeadLetter;
using Eventso.Subscription.Observing.DeadLetter;

namespace Eventso.Subscription.Hosting;

public sealed class PoisonEventRetryingHost : BackgroundService
{
    private readonly TimeSpan _reprocessingInterval;
    private readonly ILogger _logger;
    private readonly IReadOnlyCollection<TopicRetryingService> _topicRetryingServices;

    public PoisonEventRetryingHost(
        IEnumerable<ISubscriptionCollection> subscriptions,
        IMessagePipelineFactory pipelineFactory,
        IMessageHandlersRegistry handlersRegistry,
        DeadLetterQueueOptions deadLetterQueueOptions,
        PoisonEventManagerFactory poisonEventManagerFactory,
        IDeadLetterQueueScopeFactory deadLetterQueueScopeFactory,
        ILoggerFactory loggerFactory)
    {
        _reprocessingInterval = deadLetterQueueOptions.ReprocessingJobInterval;
        _logger = loggerFactory.CreateLogger<SubscriptionHost>();

        _topicRetryingServices = (subscriptions ?? throw new ArgumentNullException(nameof(subscriptions)))
            .SelectMany(x => x)
            .Where(x => x.EnableDeadLetterQueue)
            .SelectMany(x => x.ClonePerConsumerInstance())
            .Select(x => CreateTopicRetryingService(x, pipelineFactory, handlersRegistry, poisonEventManagerFactory, deadLetterQueueScopeFactory, loggerFactory))
            .ToArray();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (_topicRetryingServices.Count == 0)
            return;
            
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                foreach (var topicRetryingService in _topicRetryingServices)
                    await topicRetryingService.Retry(stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Dead letter queue retrying failed.");
            }

            await Task.Delay(_reprocessingInterval, stoppingToken);
        }
    }

    private static TopicRetryingService CreateTopicRetryingService(
        SubscriptionConfiguration config,
        IMessagePipelineFactory messagePipelineFactory,
        IMessageHandlersRegistry handlersRegistry,
        PoisonEventManagerFactory poisonEventManagerFactory,
        IDeadLetterQueueScopeFactory deadLetterQueueScopeFactory,
        ILoggerFactory loggerFactory)
    {
        var valueDeserializer = new ValueDeserializer(
            new CompositeDeserializer(config.TopicConfigurations.Select(c => KeyValuePair.Create(c.Topic, c.Serializer))),
            handlersRegistry);
        var eventHandlers = config.TopicConfigurations
            .ToFrozenDictionary(
                c => c.Topic,
                // this event handler is crucial to work with both batch and single processing
                c => new Observing.EventHandler<Event>(
                    handlersRegistry,
                    messagePipelineFactory.Create(c.HandlerConfig)));

        return new TopicRetryingService(
            config.TopicConfigurations.Select(c => c.Topic).ToArray(),
            valueDeserializer,
            eventHandlers,
            deadLetterQueueScopeFactory,
            poisonEventManagerFactory.Create(
                config.Settings.Config.GroupId,
                config.SubscriptionConfigurationId)!,
            loggerFactory.CreateLogger<TopicRetryingService>());
    }
}