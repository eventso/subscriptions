using Eventso.Subscription.Kafka;
using Eventso.Subscription.Kafka.DeadLetter;
using Eventso.Subscription.Kafka.DeadLetter.Store;
using Eventso.Subscription.Observing.DeadLetter;

namespace Eventso.Subscription.Hosting;

public sealed class KafkaConsumerFactory : IConsumerFactory
{
    private readonly IMessagePipelineFactory _messagePipelineFactory;
    private readonly IMessageHandlersRegistry _handlersRegistry;
    private readonly DeadLetterQueueOptions _deadLetterQueueOptions;
    private readonly IPoisonEventStore _poisonEventStore;
    private readonly IDeadLetterQueueScopeFactory _deadLetterQueueScopeFactory;
    private readonly ILoggerFactory _loggerFactory;

    public KafkaConsumerFactory(
        IMessagePipelineFactory messagePipelineFactory,
        IMessageHandlersRegistry handlersRegistry,
        DeadLetterQueueOptions deadLetterQueueOptions,
        IPoisonEventStore poisonEventStore,
        IDeadLetterQueueScopeFactory deadLetterQueueScopeFactory,
        ILoggerFactory loggerFactory)
    {
        _messagePipelineFactory = messagePipelineFactory;
        _handlersRegistry = handlersRegistry;
        _deadLetterQueueOptions = deadLetterQueueOptions;
        _poisonEventStore = poisonEventStore;
        _deadLetterQueueScopeFactory = deadLetterQueueScopeFactory;
        _loggerFactory = loggerFactory;
    }

    public ISubscriptionConsumer CreateConsumer(SubscriptionConfiguration config)
    {
        var poisonEventInbox = config.TopicConfigurations.Any(c => c.EnableDeadLetterQueue)
            ? new PoisonEventInbox(
                _poisonEventStore,
                _deadLetterQueueOptions.MaxTopicQueueSize,
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
            poisonEventInbox,
            config.Settings,
            _loggerFactory.CreateLogger<KafkaConsumer>());
    }
}