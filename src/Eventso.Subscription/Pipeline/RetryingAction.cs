using Polly;

namespace Eventso.Subscription.Pipeline;

public sealed class RetryingAction : IMessagePipelineAction
{
    private readonly IMessagePipelineAction _next;
    private readonly IAsyncPolicy _policy;
    private readonly ILogger<RetryingAction> _logger;

    public RetryingAction(IAsyncPolicy policy, ILogger<RetryingAction> logger, IMessagePipelineAction next)
    {
        _next = next ?? throw new ArgumentNullException(nameof(next));
        _policy = policy ?? throw new ArgumentNullException(nameof(policy));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task Invoke<T>(T message, CancellationToken token)
    {
        try
        {
            await _next.Invoke(message, token);
        }
        catch (Exception ex)
        {
            var delay = TimeSpan.FromMilliseconds(100);

            LogError(ex, delay, 0);

            await Task.Delay(delay, token);

            await _policy.ExecuteAsync(ct => _next.Invoke(message, ct), token);
        }
    }

    private void LogError(Exception ex, TimeSpan timeout, int attempt)
    {
        _logger.LogError(ex,
            $"Message processing failed and will be retried. Attempt = {attempt}, timeout = {timeout}.");
    }
}