using Eventso.Subscription.Configurations;
using Microsoft.Extensions.Logging;

namespace Eventso.Subscription.Pipeline
{
    public sealed class MessagePipelineFactory : IMessagePipelineFactory
    {
        private readonly IMessageHandlerScopeFactory _scopeFactory;
        private readonly ILoggerFactory _loggerFactory;

        public MessagePipelineFactory(IMessageHandlerScopeFactory scopeFactory, ILoggerFactory loggerFactory)
        {
            _scopeFactory = scopeFactory;
            _loggerFactory = loggerFactory;
        }

        public IMessagePipelineAction Create(HandlerConfiguration config)
        {
            IMessagePipelineAction action = new MessageHandlingAction(_scopeFactory,
                config.RunHandlersInParallel);

            var logger = _loggerFactory.CreateLogger<RetryingAction>();
            action = new RetryingAction(
                config.RetryPolicy ?? DefaultRetryPolicy.CreateDefaultPolicy(logger),
                logger,
                action);

            if (config.LoggingEnabled)
                action = new LoggingAction(_loggerFactory, action);

            return action;
        }
    }
}