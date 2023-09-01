using Polly;

namespace Eventso.Subscription.Configurations;

public sealed record HandlerConfiguration(
    bool LoggingEnabled = false,
    IAsyncPolicy? RetryPolicy = default,
    bool RunHandlersInParallel = false);