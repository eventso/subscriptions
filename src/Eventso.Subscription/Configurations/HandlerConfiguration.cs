using Polly;

namespace Eventso.Subscription.Configurations;

public sealed record HandlerConfiguration(
    bool LoggingEnabled = false,
    ResiliencePipeline? ResiliencePipeline = default,
    bool RunHandlersInParallel = false);