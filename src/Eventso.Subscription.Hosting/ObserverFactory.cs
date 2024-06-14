using Eventso.Subscription.Kafka;
using Eventso.Subscription.Kafka.DeadLetter;
using Eventso.Subscription.Observing.DeadLetter;

namespace Eventso.Subscription.Hosting;

public sealed class ObserverFactory : IObserverFactory<Event>
{
    private readonly SubscriptionConfiguration _configuration;
    private readonly IMessagePipelineFactory _messagePipelineFactory;
    private readonly IMessageHandlersRegistry _messageHandlersRegistry;
    private readonly IPoisonEventQueue _poisonEventQueue;
    private readonly ILoggerFactory _loggerFactory;

    public ObserverFactory(
        SubscriptionConfiguration configuration,
        IMessagePipelineFactory messagePipelineFactory,
        IMessageHandlersRegistry messageHandlersRegistry,
        IPoisonEventQueue poisonEventQueue,
        ILoggerFactory loggerFactory)
    {
        _configuration = configuration;
        _messagePipelineFactory = messagePipelineFactory;
        _messageHandlersRegistry = messageHandlersRegistry;
        _poisonEventQueue = poisonEventQueue;
        _loggerFactory = loggerFactory;
    }


    public IObserver<Event> Create(IConsumer<Event> consumer, string topic)
    {
        var topicConfig = _configuration.GetByTopic(topic);

        IEventHandler<Event> eventHandler = new Observing.EventHandler<Event>(
            _messageHandlersRegistry,
            _messagePipelineFactory.Create(topicConfig.HandlerConfig));

        eventHandler = new LoggingScopeEventHandler<Event>(eventHandler, topic, _loggerFactory.CreateLogger("EventHandler"));

        if (_poisonEventQueue.IsEnabled)
        {
            eventHandler = new PoisonEventHandler<Event>(
                new PoisonEventInbox(
                    _poisonEventQueue,
                    _configuration.Settings,
                    topic,
                    _loggerFactory.CreateLogger<PoisonEventInbox>()),
                eventHandler,
                _loggerFactory.CreateLogger<PoisonEventHandler<Event>>());
        }

        var observer = topicConfig.BatchProcessingRequired
            ? CreateBatchEventObserver(consumer, eventHandler, topicConfig)
            : CreateSingleEventObserver(consumer, eventHandler, topicConfig);

        if (topicConfig.ObservingDelay is { Ticks: > 0 })
            observer = new DelayedEventObserver<Event>(topicConfig.ObservingDelay.Value, observer);

        if (topicConfig.BufferSize > 0)
            observer = new BufferedObserver<Event>(topicConfig.BufferSize, observer, consumer.CancellationToken);

        return observer;
    }

    private BatchEventObserver<Event> CreateBatchEventObserver(
        IConsumer<Event> consumer,
        IEventHandler<Event> eventHandler,
        TopicSubscriptionConfiguration configuration)
    {
        return new BatchEventObserver<Event>(
            configuration.BatchConfiguration!,
            configuration.BatchConfiguration!.HandlingStrategy switch
            {
                BatchHandlingStrategy.SingleType => eventHandler,
                BatchHandlingStrategy.SingleTypeLastByKey => new SingleTypeLastByKeyEventHandler<Event>(
                    eventHandler),
                BatchHandlingStrategy.OrderedWithinKey => new OrderedWithinKeyEventHandler<Event>(eventHandler),
                BatchHandlingStrategy.OrderedWithinType => new OrderedWithinTypeEventHandler<Event>(eventHandler),
                _ => throw new InvalidOperationException(
                    $"Unknown handling strategy: {configuration.BatchConfiguration.HandlingStrategy}")
            },
            consumer,
            _messageHandlersRegistry,
            _loggerFactory.CreateLogger<BatchEventObserver<Event>>(),
            configuration.SkipUnknownMessages);
    }

    private IObserver<Event> CreateSingleEventObserver(
        IConsumer<Event> consumer,
        IEventHandler<Event> eventHandler,
        TopicSubscriptionConfiguration configuration)
    {
        return new EventObserver<Event>(
            eventHandler,
            consumer,
            _messageHandlersRegistry,
            configuration.SkipUnknownMessages,
            configuration.DeferredAckConfiguration!);
    }
}