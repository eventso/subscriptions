using Polly;

namespace Eventso.Subscription.Configurations;

public sealed record HandlerConfiguration(
    bool LoggingEnabled = false,
    ResiliencePipeline? ResiliencePipeline = default,
    ResiliencePipeline? BatchSliceResiliencePipeline = default,
    bool RunHandlersInParallel = false);