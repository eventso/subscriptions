using Eventso.Subscription.Hosting;
using Microsoft.Extensions.Logging;

namespace Eventso.Subscription.Kafka.DeadLetter;

public sealed class DeadLetterKafkaConsumerFactory(
    IMessagePipelineFactory messagePipelineFactory,
    IMessageHandlersRegistry handlersRegistry,
    IPoisonEventQueueFactory poisonEventQueueFactory,
    ILoggerFactory loggerFactory)
    : IConsumerFactory
{
    public ISubscriptionConsumer CreateConsumer(SubscriptionConfiguration config)
    {
        var poisonEventQueue = poisonEventQueueFactory.Create(
            config.Settings.Config.GroupId,
            config.SubscriptionConfigurationId);

        var poisonEventInboxFactory = new PoisonEventInboxFactory(
            poisonEventQueue,
            config.Settings,
            loggerFactory);

        var logger = loggerFactory.CreateLogger<KafkaConsumer>();

        return new KafkaConsumer(
            config.GetTopics(),
            new DeadLetterObserverFactory<Event>(
                config,
                messagePipelineFactory,
                handlersRegistry,
                poisonEventInboxFactory,
                loggerFactory),
            new ValueDeserializer(
                new CompositeDeserializer(config.TopicConfigurations.Select(c => KeyValuePair.Create(c.Topic, c.Serializer))),
                handlersRegistry),
            new LoggingConsumerEventsObserver(logger, new PoisonEventQueueConsumerObserver(poisonEventQueue)),
            config.Settings,
            logger);
    }
}