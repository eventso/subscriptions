namespace Eventso.Subscription.Hosting;

public class ObserverFactory<TEvent>(
    SubscriptionConfiguration configuration,
    IMessagePipelineFactory messagePipelineFactory,
    IMessageHandlersRegistry messageHandlersRegistry,
    ILoggerFactory loggerFactory) : IObserverFactory<TEvent> where TEvent : IEvent
{
    public IObserver<TEvent> Create(IConsumer<TEvent> consumer, string topic)
    {
        var topicConfig = configuration.GetByTopic(topic);

        var eventHandler =
            new LoggingScopeEventHandler<TEvent>(CreateBaseHandler(topicConfig), topic, loggerFactory.CreateLogger("EventHandler"));

        var observer = topicConfig.BatchProcessingRequired
            ? CreateBatchEventObserver(consumer, eventHandler, topicConfig)
            : CreateSingleEventObserver(consumer, eventHandler, topicConfig);

        if (topicConfig.ObservingDelay is { Ticks: > 0 })
            observer = new DelayedEventObserver<TEvent>(topicConfig.ObservingDelay.Value, observer);

        if (topicConfig.BufferSize > 0)
            observer = new BufferedObserver<TEvent>(topicConfig.BufferSize, observer, consumer.CancellationToken);

        return observer;
    }

    protected virtual IEventHandler<TEvent> CreateBaseHandler(TopicSubscriptionConfiguration topicConfiguration)
    {
        return new Observing.EventHandler<TEvent>(
            messageHandlersRegistry,
            messagePipelineFactory.Create(topicConfiguration.HandlerConfig));
    }

    private BatchEventObserver<TEvent> CreateBatchEventObserver(
        IConsumer<TEvent> consumer,
        IEventHandler<TEvent> eventHandler,
        TopicSubscriptionConfiguration topicConfiguration)
    {
        return new BatchEventObserver<TEvent>(
            topicConfiguration.BatchConfiguration!,
            topicConfiguration.BatchConfiguration!.HandlingStrategy switch
            {
                BatchHandlingStrategy.SingleType => eventHandler,
                BatchHandlingStrategy.SingleTypeLastByKey => new SingleTypeLastByKeyEventHandler<TEvent>(eventHandler),
                BatchHandlingStrategy.OrderedWithinKey => new OrderedWithinKeyEventHandler<TEvent>(eventHandler),
                BatchHandlingStrategy.OrderedWithinType => new OrderedWithinTypeEventHandler<TEvent>(eventHandler),
                _ => throw new InvalidOperationException($"Unknown handling strategy: {topicConfiguration.BatchConfiguration.HandlingStrategy}")
            },
            consumer,
            messageHandlersRegistry,
            loggerFactory.CreateLogger<BatchEventObserver<TEvent>>(),
            topicConfiguration.SkipUnknownMessages);
    }

    private IObserver<TEvent> CreateSingleEventObserver(
        IConsumer<TEvent> consumer,
        IEventHandler<TEvent> eventHandler,
        TopicSubscriptionConfiguration topicConfiguration)
    {
        return new EventObserver<TEvent>(
            eventHandler,
            consumer,
            messageHandlersRegistry,
            topicConfiguration.SkipUnknownMessages);
    }
}