using Microsoft.Extensions.Configuration;

namespace Eventso.Subscription.Hosting;

public sealed class ObserverFactory : IObserverFactory
{
    private readonly SubscriptionConfiguration _configuration;
    private readonly IMessagePipelineFactory _messagePipelineFactory;
    private readonly IMessageHandlersRegistry _messageHandlersRegistry;
    private readonly ILoggerFactory _loggerFactory;

    public ObserverFactory(
        SubscriptionConfiguration configuration,
        IMessagePipelineFactory messagePipelineFactory,
        IMessageHandlersRegistry messageHandlersRegistry,
        ILoggerFactory loggerFactory)
    {
        _configuration = configuration;
        _messagePipelineFactory = messagePipelineFactory;
        _messageHandlersRegistry = messageHandlersRegistry;
        _loggerFactory = loggerFactory;
    }

    public IObserver<TEvent> Create<TEvent>(IConsumer<TEvent> consumer, string topic)
        where TEvent : IEvent
    {
        var topicConfig = _configuration.GetByTopic(topic);

        IEventHandler<TEvent> eventHandler = new Observing.EventHandler<TEvent>(
            _messageHandlersRegistry,
            _messagePipelineFactory.Create(topicConfig.HandlerConfig));

        eventHandler = new LoggingScopeEventHandler<TEvent>(eventHandler, topic, _loggerFactory.CreateLogger("EventHandler"));

        var observer = topicConfig.BatchProcessingRequired
            ? CreateBatchEventObserver(consumer, eventHandler, topicConfig)
            : CreateSingleEventObserver(consumer, eventHandler, topicConfig);

        if (topicConfig.ObservingDelay is { Ticks: > 0 })
            observer = new DelayedEventObserver<TEvent>(topicConfig.ObservingDelay.Value, observer);

        if (topicConfig.BufferSize > 0)
            observer = new BufferedObserver<TEvent>(topicConfig.BufferSize, observer, consumer.CancellationToken);

        return observer;
    }

    private BatchEventObserver<TEvent> CreateBatchEventObserver<TEvent>(
        IConsumer<TEvent> consumer,
        IEventHandler<TEvent> eventHandler,
        TopicSubscriptionConfiguration configuration)
        where TEvent : IEvent
    {
        return new BatchEventObserver<TEvent>(
            configuration.BatchConfiguration!,
            configuration.BatchConfiguration!.HandlingStrategy switch
            {
                BatchHandlingStrategy.SingleType => eventHandler,
                BatchHandlingStrategy.SingleTypeLastByKey => new SingleTypeLastByKeyEventHandler<TEvent>(
                    eventHandler),
                BatchHandlingStrategy.OrderedWithinKey => new OrderedWithinKeyEventHandler<TEvent>(eventHandler),
                BatchHandlingStrategy.OrderedWithinType => new OrderedWithinTypeEventHandler<TEvent>(eventHandler),
                _ => throw new InvalidOperationException(
                    $"Unknown handling strategy: {configuration.BatchConfiguration.HandlingStrategy}")
            },
            consumer,
            _messageHandlersRegistry,
            _loggerFactory.CreateLogger<BatchEventObserver<TEvent>>(),
            configuration.SkipUnknownMessages);
    }

    private IObserver<TEvent> CreateSingleEventObserver<TEvent>(
        IConsumer<TEvent> consumer,
        IEventHandler<TEvent> eventHandler,
        TopicSubscriptionConfiguration configuration)
        where TEvent : IEvent
    {
        return new EventObserver<TEvent>(
            eventHandler,
            consumer,
            _messageHandlersRegistry,
            configuration.SkipUnknownMessages,
            configuration.DeferredAckConfiguration!);
    }
}