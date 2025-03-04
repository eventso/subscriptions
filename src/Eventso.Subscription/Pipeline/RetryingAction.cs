using Polly;

namespace Eventso.Subscription.Pipeline;

public sealed class RetryingAction(
    ResiliencePipeline resiliencePipeline,
    ResiliencePipeline batchSliceResiliencePipeline,
    IMessagePipelineAction next) : IMessagePipelineAction
{
    public async Task Invoke<T>(T message, HandlingContext context, CancellationToken token) where T : notnull
    {
        if (context.IsBatchSlice)
        {
            await batchSliceResiliencePipeline.ExecuteAsync(
                static async (p, ct) => await p._next.Invoke(p.message, p.context, ct), (_next: next, context, message), token);
        }
        else
        {
            await resiliencePipeline.ExecuteAsync(
                static async (p, ct) => await p._next.Invoke(p.message, p.context, ct), (_next: next, context, message), token);
        }
    }
}