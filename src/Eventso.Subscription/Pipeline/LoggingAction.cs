namespace Eventso.Subscription.Pipeline;

public sealed class LoggingAction : IMessagePipelineAction
{
    private readonly IMessagePipelineAction _next;
    private readonly ILogger<LoggingAction> _logger;

    public LoggingAction(ILoggerFactory factory, IMessagePipelineAction next)
    {
        _next = next ?? throw new ArgumentNullException(nameof(next));
        _logger = factory.CreateLogger<LoggingAction>();
    }

    public Task Invoke<T>(T message, CancellationToken token) where T: notnull
    {
        _logger.LogInformation($"Message received: {message.GetType().Name}.");

        return _next.Invoke(message, token);
    }
}