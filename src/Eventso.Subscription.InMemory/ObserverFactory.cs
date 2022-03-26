using System;
using Eventso.Subscription.Configurations;
using Eventso.Subscription.InMemory.Hosting;
using Eventso.Subscription.Observing;
using Eventso.Subscription.Observing.Batch;
using Microsoft.Extensions.Logging.Abstractions;

namespace Eventso.Subscription.InMemory
{
    public sealed class ObserverFactory : IObserverFactory
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

        public IObserver<T> Create<T>(IConsumer<T> consumer)
            where T : IEvent
        {
            var pipelineAction = _messagePipelineFactory.Create(_configuration.HandlerConfiguration);

            if (_configuration.BatchProcessingRequired)
            {
                return new BatchEventObserver<T>(
                    _configuration.BatchConfiguration,
                    GetBatchHandler(),
                    consumer,
                    _messageHandlersRegistry,
                    skipUnknown: true);
            }

            return new EventObserver<T>(
                pipelineAction,
                consumer,
                _messageHandlersRegistry,
                skipUnknown: true,
                _configuration.DeferredAckConfiguration,
                NullLogger<EventObserver<T>>.Instance);

            IBatchHandler<T> GetBatchHandler()
            {
                var batchPipelineAction = new MessageBatchPipelineAction(
                    _messageHandlersRegistry,
                    pipelineAction);

                var singleTypeHandler = new SingleTypeBatchHandler<T>(batchPipelineAction);

                return _configuration.BatchConfiguration.HandlingStrategy switch
                {
                    BatchHandlingStrategy.SingleType
                        => singleTypeHandler,

                    BatchHandlingStrategy.SingleTypeLastByKey
                        => new SingleTypeLastByKeyBatchHandler<T>(batchPipelineAction),

                    BatchHandlingStrategy.OrderedWithinKey
                        => new OrderedWithinKeyBatchHandler<T>(batchPipelineAction),

                    BatchHandlingStrategy.OrderedWithinType =>
                        new OrderedWithinTypeBatchHandler<T>(batchPipelineAction),

                    _ => throw new InvalidOperationException(
                        $"Unknown handling strategy: {_configuration.BatchConfiguration.HandlingStrategy}")
                };
            }
        }
    }
}