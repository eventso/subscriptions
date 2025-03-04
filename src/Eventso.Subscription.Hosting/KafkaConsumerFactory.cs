using Eventso.Subscription.Kafka;

namespace Eventso.Subscription.Hosting;

public sealed class KafkaConsumerFactory : IConsumerFactory
{
    private readonly IMessagePipelineFactory _messagePipelineFactory;
    private readonly IMessageHandlersRegistry _handlersRegistry;
    private readonly ILoggerFactory _loggerFactory;

    public KafkaConsumerFactory(
        IMessagePipelineFactory messagePipelineFactory,
        IMessageHandlersRegistry handlersRegistry,
        ILoggerFactory loggerFactory)
    {
        _messagePipelineFactory = messagePipelineFactory;
        _handlersRegistry = handlersRegistry;
        _loggerFactory = loggerFactory;
    }

    public ISubscriptionConsumer CreateConsumer(SubscriptionConfiguration config)
    {
        var logger = _loggerFactory.CreateLogger<KafkaConsumer>();

        return new KafkaConsumer(
            config.GetTopics(),
            new ObserverFactory<Event>(
                config,
                _messagePipelineFactory,
                _handlersRegistry,
                _loggerFactory),
            new ValueDeserializer(
                new CompositeDeserializer(config.TopicConfigurations.Select(c => KeyValuePair.Create(c.Topic, c.Serializer))),
                _handlersRegistry),
            new LoggingConsumerEventsObserver(logger, NullConsumerEventsObserver.Instance),
            config.Settings,
            logger);
    }
}