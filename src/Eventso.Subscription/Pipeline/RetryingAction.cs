using Polly;

namespace Eventso.Subscription.Pipeline;

public sealed class RetryingAction : IMessagePipelineAction
{
    private readonly IMessagePipelineAction _next;
    private readonly ResiliencePipeline _resiliencePipeline;

    public RetryingAction(ResiliencePipeline resiliencePipeline, IMessagePipelineAction next)
    {
        _next = next ?? throw new ArgumentNullException(nameof(next));
        _resiliencePipeline = resiliencePipeline ?? throw new ArgumentNullException(nameof(resiliencePipeline));
    }

    public async Task Invoke<T>(T message, CancellationToken token) where T : notnull
    {
        await _resiliencePipeline.ExecuteAsync(
            static async (p, ct) => await p._next.Invoke(p.message, ct), (_next, message), token);
    }
}