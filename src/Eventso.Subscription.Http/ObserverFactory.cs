using Eventso.Subscription.Configurations;
using Eventso.Subscription.Http.Hosting;
using Eventso.Subscription.Observing;
using Eventso.Subscription.Observing.Batch;
using Microsoft.Extensions.Logging.Abstractions;

namespace Eventso.Subscription.Http;

public sealed class ObserverFactory<TEvent> : IObserverFactory<TEvent>
    where TEvent : IEvent
{
    private readonly SubscriptionConfiguration _configuration;
    private readonly IMessagePipelineFactory _messagePipelineFactory;
    private readonly IMessageHandlersRegistry _messageHandlersRegistry;

    public ObserverFactory(
        SubscriptionConfiguration configuration,
        IMessagePipelineFactory messagePipelineFactory,
        IMessageHandlersRegistry messageHandlersRegistry)
    {
        _configuration = configuration;
        _messagePipelineFactory = messagePipelineFactory;
        _messageHandlersRegistry = messageHandlersRegistry;
    }

    public IObserver<TEvent> Create(IConsumer<TEvent> consumer, string topic)
    {
        var eventHandler = new Observing.EventHandler<TEvent>(
            _messageHandlersRegistry,
            _messagePipelineFactory.Create(_configuration.HandlerConfiguration, withDlq: false));

        if (_configuration.BatchProcessingRequired)
        {
            return new BatchEventObserver<TEvent>(
                _configuration.BatchConfiguration!,
                GetBatchHandler(),
                consumer,
                _messageHandlersRegistry,
                NullLogger<BatchEventObserver<TEvent>>.Instance,
                skipUnknown: true);
        }

        return new EventObserver<TEvent>(
            eventHandler,
            consumer,
            _messageHandlersRegistry,
            skipUnknown: true);

        IEventHandler<TEvent> GetBatchHandler()
        {
            return _configuration.BatchConfiguration!.HandlingStrategy switch
            {
                BatchHandlingStrategy.SingleType
                    => eventHandler,
                BatchHandlingStrategy.SingleTypeLastByKey
                    => new SingleTypeLastByKeyEventHandler<TEvent>(eventHandler),
                BatchHandlingStrategy.OrderedWithinKey
                    => new OrderedWithinKeyEventHandler<TEvent>(eventHandler),
                BatchHandlingStrategy.OrderedWithinType =>
                    new OrderedWithinTypeEventHandler<TEvent>(eventHandler),
                _ => throw new InvalidOperationException(
                    $"Unknown handling strategy: {_configuration.BatchConfiguration.HandlingStrategy}")
            };
        }
    }
}