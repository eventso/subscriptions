using System;
using Eventso.Subscription.Configurations;
using Eventso.Subscription.Observing;
using Eventso.Subscription.Observing.Batch;
using Microsoft.Extensions.Logging;

namespace Eventso.Subscription.Hosting
{
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

        public IObserver<T> Create<T>(IConsumer<T> consumer)
            where T : IEvent
        {
            var pipelineAction = _messagePipelineFactory.Create(_configuration.HandlerConfig);

            if (_configuration.BatchProcessingRequired)
            {
                return new BatchEventObserver<T>(
                    _configuration.BatchConfiguration,
                    GetBatchHandler(),
                    consumer,
                    _messageHandlersRegistry,
                    _configuration.SkipUnknownMessages);
            }

            return new EventObserver<T>(
                pipelineAction,
                consumer,
                _messageHandlersRegistry,
                _configuration.SkipUnknownMessages,
                _configuration.DeferredAckConfiguration,
                _loggerFactory.CreateLogger<EventObserver<T>>());

            IBatchHandler<T> GetBatchHandler()
            {
                var batchPipelineAction = new MessageBatchPipelineAction(
                    _messageHandlersRegistry,
                    pipelineAction);

                var singleTypeHandler = new SingleTypeBatchHandler<T>(batchPipelineAction);

                return _configuration.BatchConfiguration.HandlingStrategy switch
                {
                    BatchHandlingStrategy.SingleType => singleTypeHandler,
                    BatchHandlingStrategy.SingleTypeLastByKey => new SingleTypeLastByKeyBatchHandler<T>(batchPipelineAction),
                    BatchHandlingStrategy.OrderedWithinKey => new OrderedWithinKeyBatchHandler<T>(batchPipelineAction),
                    BatchHandlingStrategy.OrderedWithinType => new OrderedWithinTypeBatchHandler<T>(batchPipelineAction),
                    _ => throw new InvalidOperationException(
                        $"Unknown handling strategy: {_configuration.BatchConfiguration.HandlingStrategy}")
                };
            }
        }
    }
}