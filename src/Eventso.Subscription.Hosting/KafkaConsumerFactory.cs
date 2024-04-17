using Eventso.Subscription.Kafka;
using Eventso.Subscription.Kafka.DeadLetter;
using Eventso.Subscription.Observing.DeadLetter;

namespace Eventso.Subscription.Hosting;

public sealed class KafkaConsumerFactory : IConsumerFactory
{
    private readonly IMessagePipelineFactory _messagePipelineFactory;
    private readonly IMessageHandlersRegistry _handlersRegistry;
    private readonly PoisonEventManagerFactory _poisonEventManagerFactory;
    private readonly IDeadLetterQueueScopeFactory _deadLetterQueueScopeFactory;
    private readonly ILoggerFactory _loggerFactory;

    public KafkaConsumerFactory(
        IMessagePipelineFactory messagePipelineFactory,
        IMessageHandlersRegistry handlersRegistry,
        PoisonEventManagerFactory poisonEventManagerFactory,
        IDeadLetterQueueScopeFactory deadLetterQueueScopeFactory,
        ILoggerFactory loggerFactory)
    {
        _messagePipelineFactory = messagePipelineFactory;
        _handlersRegistry = handlersRegistry;
        _poisonEventManagerFactory = poisonEventManagerFactory;
        _deadLetterQueueScopeFactory = deadLetterQueueScopeFactory;
        _loggerFactory = loggerFactory;
    }

    public ISubscriptionConsumer CreateConsumer(SubscriptionConfiguration config)
    {
        var poisonEventManager = _poisonEventManagerFactory.Create(
            config.Settings.Config.GroupId,
            config.SubscriptionConfigurationId);
        var poisonEventInbox = poisonEventManager != null
            ? new PoisonEventInbox(
                poisonEventManager,
                config.Settings,
                config.GetTopics(),
                _loggerFactory.CreateLogger<PoisonEventInbox>())
            : null;
        return new KafkaConsumer(
            config.GetTopics(),
            new ObserverFactory(
                config,
                _messagePipelineFactory,
                _handlersRegistry,
                poisonEventInbox,
                _deadLetterQueueScopeFactory,
                _loggerFactory),
            new ValueDeserializer(
                new CompositeDeserializer(config.TopicConfigurations.Select(c => KeyValuePair.Create(c.Topic, c.Serializer))),
                _handlersRegistry),
            poisonEventManager,
            config.Settings,
            _loggerFactory.CreateLogger<KafkaConsumer>());
    }
}