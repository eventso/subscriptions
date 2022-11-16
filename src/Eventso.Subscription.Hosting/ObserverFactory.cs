using System;
using Eventso.Subscription.Configurations;
using Eventso.Subscription.Observing;
using Eventso.Subscription.Observing.Batch;
using Microsoft.Extensions.Logging;

namespace Eventso.Subscription.Hosting
{
    public sealed class ObserverFactory : IObserverFactory
    {
        private readonly TopicSubscriptionConfiguration _configuration;
        private readonly IMessagePipelineFactory _messagePipelineFactory;
        private readonly IMessageHandlersRegistry _messageHandlersRegistry;
        private readonly ILoggerFactory _loggerFactory;

        public ObserverFactory(
            TopicSubscriptionConfiguration configuration,
            IMessagePipelineFactory messagePipelineFactory,
            IMessageHandlersRegistry messageHandlersRegistry,
            ILoggerFactory loggerFactory) 
        {
            _configuration = configuration;
            _messagePipelineFactory = messagePipelineFactory;
            _messageHandlersRegistry = messageHandlersRegistry;
            _loggerFactory = loggerFactory;
        }

        public IObserver<TEvent> Create<TEvent>(IConsumer<TEvent> consumer)
            where TEvent : IEvent
        {
            IEventHandler<TEvent> eventHandler = new Observing.EventHandler<TEvent>(
                _messageHandlersRegistry,
                _messagePipelineFactory.Create(_configuration.HandlerConfig));

            IObserver<TEvent> observer = _configuration.BatchProcessingRequired
                ? CreateBatchEventObserver(consumer, eventHandler)
                : CreateSingleEventObserver(consumer, eventHandler);

            return observer;
        }

        private BatchEventObserver<TEvent> CreateBatchEventObserver<TEvent>(
            IConsumer<TEvent> consumer,
            IEventHandler<TEvent> eventHandler)
            where TEvent : IEvent
        {
            return new BatchEventObserver<TEvent>(
                _configuration.BatchConfiguration,
                _configuration.BatchConfiguration.HandlingStrategy switch
                {
                    BatchHandlingStrategy.SingleType => eventHandler,
                    BatchHandlingStrategy.SingleTypeLastByKey => new SingleTypeLastByKeyEventHandler<TEvent>(eventHandler),
                    BatchHandlingStrategy.OrderedWithinKey => new OrderedWithinKeyEventHandler<TEvent>(eventHandler),
                    BatchHandlingStrategy.OrderedWithinType => new OrderedWithinTypeEventHandler<TEvent>(eventHandler),
                    _ => throw new InvalidOperationException(
                        $"Unknown handling strategy: {_configuration.BatchConfiguration.HandlingStrategy}")
                },
                consumer,
                _messageHandlersRegistry,
                _configuration.SkipUnknownMessages);
        }

        private EventObserver<TEvent> CreateSingleEventObserver<TEvent>(
            IConsumer<TEvent> consumer,
            IEventHandler<TEvent> eventHandler)
            where TEvent : IEvent
        {
            return new EventObserver<TEvent>(
                eventHandler,
                consumer,
                _messageHandlersRegistry,
                _configuration.SkipUnknownMessages,
                _configuration.DeferredAckConfiguration,
                _loggerFactory.CreateLogger<EventObserver<TEvent>>());
        }
    }
}