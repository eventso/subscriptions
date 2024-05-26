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
        return new KafkaConsumer(
            config.GetTopics(),
            new ObserverFactory<Event>(
                config,
                _messagePipelineFactory,
                _handlersRegistry,
                _loggerFactory,
                KafkaGroupedMetadataProvider.Instance),
            new ValueDeserializer(
                new CompositeDeserializer(config.TopicConfigurations.Select(c => KeyValuePair.Create(c.Topic, c.Serializer))),
                _handlersRegistry),
            // TODO get some service from DI instead of default
            //config.EnableDeadLetterQueue ? default : null,
            default!,
            config.Settings,
            _loggerFactory.CreateLogger<KafkaConsumer>());
    }
}
