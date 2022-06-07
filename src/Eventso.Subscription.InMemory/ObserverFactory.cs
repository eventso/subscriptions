using System;
using Eventso.Subscription.Configurations;
using Eventso.Subscription.InMemory.Hosting;
using Eventso.Subscription.Observing;
using Eventso.Subscription.Observing.Batch;
using Microsoft.Extensions.Logging.Abstractions;

namespace Eventso.Subscription.InMemory
{
    public sealed class ObserverFactory : IObserverFactory<Event>
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

        public IObserver<Event> Create(IConsumer<Event> consumer)
        {
            var eventHandler = new Observing.EventHandler<Event>(
                _messageHandlersRegistry,
                _messagePipelineFactory.Create(_configuration.HandlerConfiguration));

            if (_configuration.BatchProcessingRequired)
            {
                return new BatchEventObserver<Event>(
                    _configuration.BatchConfiguration,
                    GetBatchHandler(),
                    consumer,
                    _messageHandlersRegistry,
                    skipUnknown: true);
            }

            return new EventObserver<Event>(
                eventHandler,
                consumer,
                _messageHandlersRegistry,
                skipUnknown: true,
                _configuration.DeferredAckConfiguration,
                NullLogger<EventObserver<Event>>.Instance);

            IEventHandler<Event> GetBatchHandler()
            {
                return _configuration.BatchConfiguration.HandlingStrategy switch
                {
                    BatchHandlingStrategy.SingleType
                        => eventHandler,
                    BatchHandlingStrategy.SingleTypeLastByKey
                        => new SingleTypeLastByKeyEventHandler<Event>(eventHandler),
                    BatchHandlingStrategy.OrderedWithinKey
                        => new OrderedWithinKeyEventHandler<Event>(eventHandler),
                    BatchHandlingStrategy.OrderedWithinType =>
                        new OrderedWithinTypeEventHandler<Event>(eventHandler),
                    _ => throw new InvalidOperationException(
                        $"Unknown handling strategy: {_configuration.BatchConfiguration.HandlingStrategy}")
                };
            }
        }
    }
}