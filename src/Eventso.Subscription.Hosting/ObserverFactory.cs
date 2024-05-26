namespace Eventso.Subscription.Hosting;

public sealed class ObserverFactory<TEvent> : IObserverFactory<TEvent> where TEvent : IEvent
{
    private readonly SubscriptionConfiguration _configuration;
    private readonly IMessagePipelineFactory _messagePipelineFactory;
    private readonly IMessageHandlersRegistry _messageHandlersRegistry;
    private readonly ILoggerFactory _loggerFactory;
    private readonly IGroupedMetadataProvider<TEvent> _groupedMetadataProvider;

    public ObserverFactory(SubscriptionConfiguration configuration,
        IMessagePipelineFactory messagePipelineFactory,
        IMessageHandlersRegistry messageHandlersRegistry,
        ILoggerFactory loggerFactory,
        IGroupedMetadataProvider<TEvent> groupedMetadataProvider)
    {
        _configuration = configuration;
        _messagePipelineFactory = messagePipelineFactory;
        _messageHandlersRegistry = messageHandlersRegistry;
        _loggerFactory = loggerFactory;
        _groupedMetadataProvider = groupedMetadataProvider;
    }

    public IObserver<TEvent> Create(IConsumer<TEvent> consumer, string topic)
    {
        var topicConfig = _configuration.GetByTopic(topic);

        IEventHandler<TEvent> eventHandler = new Observing.EventHandler<TEvent>(
            _messageHandlersRegistry,
            _messagePipelineFactory.Create(topicConfig.HandlerConfig));

        eventHandler = new ObservabilityEventHandler<TEvent>(
            eventHandler,
            _loggerFactory.CreateLogger("EventHandler"),
            _groupedMetadataProvider);

        var observer = topicConfig.BatchProcessingRequired
            ? CreateBatchEventObserver(consumer, eventHandler, topicConfig)
            : CreateSingleEventObserver(consumer, eventHandler, topicConfig);

        if (topicConfig.ObservingDelay is { Ticks: > 0 })
            observer = new DelayedEventObserver<TEvent>(topicConfig.ObservingDelay.Value, observer);

        if (topicConfig.BufferSize > 0)
            observer = new BufferedObserver<TEvent>(topicConfig.BufferSize, observer, consumer.CancellationToken);

        return observer;
    }

    private BatchEventObserver<TEvent> CreateBatchEventObserver(
        IConsumer<TEvent> consumer,
        IEventHandler<TEvent> eventHandler,
        TopicSubscriptionConfiguration configuration)
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

    private IObserver<TEvent> CreateSingleEventObserver(
        IConsumer<TEvent> consumer,
        IEventHandler<TEvent> eventHandler,
        TopicSubscriptionConfiguration configuration)
    {
        return new EventObserver<TEvent>(
            eventHandler,
            consumer,
            _messageHandlersRegistry,
            configuration.SkipUnknownMessages,
            configuration.DeferredAckConfiguration!);
    }
}
