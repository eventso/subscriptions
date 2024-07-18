using Polly;

namespace Eventso.Subscription.Pipeline;

public sealed class RetryingAction : IMessagePipelineAction
{
    private readonly IMessagePipelineAction _next;
    private readonly ResiliencePipeline _resiliencePipeline;
    private readonly ResiliencePipeline _batchSplitPartResiliencePipeline;

    public RetryingAction(
        ResiliencePipeline resiliencePipeline,
        ResiliencePipeline batchSplitPartResiliencePipeline,
        IMessagePipelineAction next)
    {
        _next = next;
        _resiliencePipeline = resiliencePipeline;
        _batchSplitPartResiliencePipeline = batchSplitPartResiliencePipeline;
    }

    public async Task Invoke<T>(T message, HandlingContext context, CancellationToken token) where T : notnull
    {
        if (context.IsBatchSplitPart)
        {
            await _batchSplitPartResiliencePipeline.ExecuteAsync(
                static async (p, ct) => await p._next.Invoke(p.message, p.context, ct), (_next, context, message), token);
        }
        else
        {
            await _resiliencePipeline.ExecuteAsync(
                static async (p, ct) => await p._next.Invoke(p.message, p.context, ct), (_next, context, message), token);
        }
    }
}