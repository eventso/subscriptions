using Eventso.Subscription.Configurations;
using Polly;

namespace Eventso.Subscription.Pipeline;

public sealed class MessagePipelineFactory : IMessagePipelineFactory
{
    private readonly IMessageHandlerScopeFactory _scopeFactory;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ResiliencePipeline _defaultPipeline;
    private readonly ResiliencePipeline _defaultShortRetryPipeline;

    public MessagePipelineFactory(
        IMessageHandlerScopeFactory scopeFactory,
        ILoggerFactory loggerFactory)
    {
        _scopeFactory = scopeFactory;
        _loggerFactory = loggerFactory;

        var logger = _loggerFactory.CreateLogger<RetryingAction>();
        _defaultPipeline = DefaultRetryingStrategy.GetDefaultBuilder(logger).Build();
        _defaultShortRetryPipeline = DefaultRetryingStrategy.GetDefaultShortRetryBuilder(logger).Build();
    }

    public IMessagePipelineAction Create(HandlerConfiguration config, bool withDlq)
    {
        var splitRetryingPipeline = (withDlq ? _defaultShortRetryPipeline : _defaultPipeline);

        IMessagePipelineAction action = new RetryingAction(
            config.ResiliencePipeline ?? _defaultPipeline,
            config.BatchSliceResiliencePipeline ?? splitRetryingPipeline,
            new MessageHandlingAction(_scopeFactory, config.RunHandlersInParallel));

        if (config.LoggingEnabled)
            action = new LoggingAction(_loggerFactory, action);

        return action;
    }
}