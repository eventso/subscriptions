using Eventso.Subscription.Configurations;
using Polly;

namespace Eventso.Subscription.Pipeline;

public sealed class MessagePipelineFactory : IMessagePipelineFactory
{
    private readonly IMessageHandlerScopeFactory _scopeFactory;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ResiliencePipeline _defaultPipeline;

    public MessagePipelineFactory(IMessageHandlerScopeFactory scopeFactory, ILoggerFactory loggerFactory)
    {
        _scopeFactory = scopeFactory;
        _loggerFactory = loggerFactory;

        var logger = _loggerFactory.CreateLogger<RetryingAction>();
        _defaultPipeline = DefaultRetryingStrategy.GetDefaultBuilder(logger).Build();
    }

    public IMessagePipelineAction Create(HandlerConfiguration config)
    {
        IMessagePipelineAction action = new MessageHandlingAction(_scopeFactory,
            config.RunHandlersInParallel);

        action = new RetryingAction(
            config.ResiliencePipeline ?? _defaultPipeline,
            action);

        if (config.LoggingEnabled)
            action = new LoggingAction(_loggerFactory, action);

        return action;
    }
}