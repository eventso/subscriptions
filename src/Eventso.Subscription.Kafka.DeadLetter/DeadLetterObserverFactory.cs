using Eventso.Subscription.Hosting;
using Microsoft.Extensions.Logging;

namespace Eventso.Subscription.Kafka.DeadLetter;

public sealed class DeadLetterObserverFactory<TEvent>(
    SubscriptionConfiguration configuration,
    IMessagePipelineFactory messagePipelineFactory,
    IMessageHandlersRegistry messageHandlersRegistry,
    IPoisonEventInboxFactory<TEvent> poisonEventInboxFactory,
    ILoggerFactory loggerFactory)
    : ObserverFactory<TEvent>(configuration, messagePipelineFactory, messageHandlersRegistry, loggerFactory) where TEvent : IEvent
{
    protected override IEventHandler<TEvent> CreateBaseHandler(TopicSubscriptionConfiguration topicConfiguration)
    {
        var baseHandler = base.CreateBaseHandler(topicConfiguration);

        return new PoisonEventHandler<TEvent>(
            topicConfiguration.Topic,
            poisonEventInboxFactory.Create(topicConfiguration.Topic),
            baseHandler);
    }
}