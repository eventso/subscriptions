using Eventso.Subscription.Kafka;

namespace Eventso.Subscription.Hosting;

public sealed class KafkaConsumerFactory : IConsumerFactory
{
    private readonly IMessagePipelineFactory _messagePipelineFactory;
    private readonly IMessageHandlersRegistry _handlersRegistry;
    private readonly IPoisonEventQueueFactory _poisonEventQueueFactory;
    private readonly ILoggerFactory _loggerFactory;

    public KafkaConsumerFactory(
        IMessagePipelineFactory messagePipelineFactory,
        IMessageHandlersRegistry handlersRegistry,
        IPoisonEventQueueFactory poisonEventQueueFactory,
        ILoggerFactory loggerFactory)
    {
        _messagePipelineFactory = messagePipelineFactory;
        _handlersRegistry = handlersRegistry;
        _poisonEventQueueFactory = poisonEventQueueFactory;
        _loggerFactory = loggerFactory;
        
    }

    public ISubscriptionConsumer CreateConsumer(SubscriptionConfiguration config)
    {
        
        var poisonEventQueue = _poisonEventQueueFactory.Create(
            config.Settings.Config.GroupId,
            config.SubscriptionConfigurationId);

        var poisonEventInboxFactory = new PoisonEventInboxFactory(
            poisonEventQueue,
            config.Settings,
            _loggerFactory);
        return new KafkaConsumer(
            config.GetTopics(),
            new ObserverFactory<Event>(
                config,
                _messagePipelineFactory,
                _handlersRegistry,
                poisonEventQueue,
                poisonEventInboxFactory,
                _loggerFactory),
            new ValueDeserializer(
                new CompositeDeserializer(config.TopicConfigurations.Select(c => KeyValuePair.Create(c.Topic, c.Serializer))),
                _handlersRegistry),
            poisonEventQueue,
            config.Settings,
            _loggerFactory.CreateLogger<KafkaConsumer>());
    }
}