using Eventso.Subscription.Configurations;
using Polly;

namespace Eventso.Subscription.Pipeline;

public sealed class MessagePipelineFactory : IMessagePipelineFactory
{
    private readonly IMessageHandlerScopeFactory _scopeFactory;
    private readonly ResiliencePipeline _resiliencePipeline;
    private readonly ILoggerFactory _loggerFactory;

    public MessagePipelineFactory(
        IMessageHandlerScopeFactory scopeFactory,
        ResiliencePipeline resiliencePipeline,
        ILoggerFactory loggerFactory)
    {
        _scopeFactory = scopeFactory;
        _resiliencePipeline = resiliencePipeline;
        _loggerFactory = loggerFactory;

        var logger = _loggerFactory.CreateLogger<RetryingAction>();
        //_defaultPipeline = DefaultRetryingStrategy.GetDefaultBuilder(logger).Build();
        //_defaultShortRetryPipeline = DefaultRetryingStrategy.GetDefaultShortRetryBuilder(logger).Build();
    }

    public IMessagePipelineAction Create(HandlerConfiguration config)
    {
        IMessagePipelineAction action = new RetryingAction(
            config.ResiliencePipeline ?? _resiliencePipeline,
            config.BatchSliceResiliencePipeline ?? _resiliencePipeline,
            new MessageHandlingAction(_scopeFactory, config.RunHandlersInParallel));

        if (config.LoggingEnabled)
            action = new LoggingAction(_loggerFactory, action);

        return action;
    }
}