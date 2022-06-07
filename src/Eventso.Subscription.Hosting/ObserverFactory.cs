using System;
using Eventso.Subscription.Configurations;
using Eventso.Subscription.Kafka;
using Eventso.Subscription.Kafka.DeadLetter;
using Eventso.Subscription.Kafka.DeadLetter.Store;
using Eventso.Subscription.Observing;
using Eventso.Subscription.Observing.Batch;
using Eventso.Subscription.Observing.DeadLetter;
using Microsoft.Extensions.Logging;

namespace Eventso.Subscription.Hosting
{
    public sealed class ObserverFactory : IObserverFactory<Event>
    {
        private readonly SubscriptionConfiguration _configuration;
        private readonly IMessagePipelineFactory _messagePipelineFactory;
        private readonly IMessageHandlersRegistry _messageHandlersRegistry;
        private readonly IPoisonEventInbox<Event> _poisonEventInbox;
        private readonly IDeadLetterQueueScopeFactory _deadLetterQueueScopeFactory;
        private readonly ILoggerFactory _loggerFactory;

        public ObserverFactory(
            SubscriptionConfiguration configuration,
            IMessagePipelineFactory messagePipelineFactory,
            IMessageHandlersRegistry messageHandlersRegistry,
            IPoisonEventInbox<Event> poisonEventInbox,
            IDeadLetterQueueScopeFactory deadLetterQueueScopeFactory,
            ILoggerFactory loggerFactory) 
        {
            _configuration = configuration;
            _messagePipelineFactory = messagePipelineFactory;
            _messageHandlersRegistry = messageHandlersRegistry;
            _poisonEventInbox = poisonEventInbox;
            _deadLetterQueueScopeFactory = deadLetterQueueScopeFactory;
            _loggerFactory = loggerFactory;
        }

        public IObserver<Event> Create(IConsumer<Event> consumer)
        {
            IEventHandler<Event> eventHandler = new Observing.EventHandler<Event>(
                _messageHandlersRegistry,
                _messagePipelineFactory.Create(_configuration.HandlerConfig));

            if (_poisonEventInbox != null)
                eventHandler = new PoisonEventHandler<Event>(
                    _poisonEventInbox,
                    _deadLetterQueueScopeFactory,
                    eventHandler);

            IObserver<Event> observer = _configuration.BatchProcessingRequired
                ? CreateBatchEventObserver(consumer, eventHandler)
                : CreateSingleEventObserver(consumer, eventHandler);

            return observer;
        }

        private BatchEventObserver<Event> CreateBatchEventObserver(
            IConsumer<Event> consumer,
            IEventHandler<Event> eventHandler)
        {
            return new BatchEventObserver<Event>(
                _configuration.BatchConfiguration,
                _configuration.BatchConfiguration.HandlingStrategy switch
                {
                    BatchHandlingStrategy.SingleType => eventHandler,
                    BatchHandlingStrategy.SingleTypeLastByKey => new SingleTypeLastByKeyEventHandler<Event>(eventHandler),
                    BatchHandlingStrategy.OrderedWithinKey => new OrderedWithinKeyEventHandler<Event>(eventHandler),
                    BatchHandlingStrategy.OrderedWithinType => new OrderedWithinTypeEventHandler<Event>(eventHandler),
                    _ => throw new InvalidOperationException(
                        $"Unknown handling strategy: {_configuration.BatchConfiguration.HandlingStrategy}")
                },
                consumer,
                _messageHandlersRegistry,
                _configuration.SkipUnknownMessages);
        }

        private EventObserver<Event> CreateSingleEventObserver(
            IConsumer<Event> consumer,
            IEventHandler<Event> eventHandler)
        {
            return new EventObserver<Event>(
                eventHandler,
                consumer,
                _messageHandlersRegistry,
                _configuration.SkipUnknownMessages,
                _configuration.DeferredAckConfiguration,
                _loggerFactory.CreateLogger<EventObserver<Event>>());
        }
    }
}