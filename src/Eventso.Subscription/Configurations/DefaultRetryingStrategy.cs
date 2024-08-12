using Polly;
using Polly.Retry;
using Polly.Timeout;

namespace Eventso.Subscription.Configurations;

public static class DefaultRetryingStrategy
{
    private const int FirstLevelRetryAttemptsCount = 9;
    private const int LongRetryDelayMinutes = 10;
    private const int LongRetryDurationMinutes = 3 * 60;

    public static ResiliencePipelineBuilder GetDefaultBuilder(ILogger logger)
    {
        return new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = LongRetryDurationMinutes / LongRetryDelayMinutes + FirstLevelRetryAttemptsCount,
                DelayGenerator = static args =>
                {
                    var delay = args.AttemptNumber switch
                    {
                        0 => TimeSpan.FromMilliseconds(100),
                        <= FirstLevelRetryAttemptsCount => TimeSpan.FromSeconds(Math.Pow(2, args.AttemptNumber)),
                        _ => TimeSpan.FromMinutes(LongRetryDelayMinutes),
                    };
                    return new ValueTask<TimeSpan?>(delay);
                },
                OnRetry = arg =>
                {
                    logger.LogError(arg.Outcome.Exception,
                        $"Message processing failed and will be retried. Attempt #{arg.AttemptNumber}, timeout: {arg.RetryDelay}, duration: {arg.Duration}.");

                    return ValueTask.CompletedTask;
                }
            });
    }

    public static ResiliencePipelineBuilder GetDefaultBuilder(ILogger logger, TimeSpan pipelineTimeout)
    {
        return GetDefaultBuilder(logger)
            .AddTimeout(new TimeoutStrategyOptions
            {
                Timeout = pipelineTimeout,
                OnTimeout = arg =>
                {
                    logger.LogWarning(
                        $"Message processing timed out and will be cancelled. Timeout: {arg.Timeout}");

                    return ValueTask.CompletedTask;
                }
            });
    }

    public static ResiliencePipelineBuilder GetDefaultShortRetryBuilder(ILogger logger)
    {
        return new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = 3,
                Delay = TimeSpan.FromMilliseconds(300),
                OnRetry = arg =>
                {
                    logger.LogError(arg.Outcome.Exception,
                        $"Message processing failed and will be retried for split part. Attempt #{arg.AttemptNumber}, timeout: {arg.RetryDelay}, duration: {arg.Duration}. ");

                    return ValueTask.CompletedTask;
                }
            });
    }

    public static ResiliencePipelineBuilder GetDefaultShortRetryBuilder(ILogger logger, TimeSpan pipelineTimeout)
    {
        return GetDefaultShortRetryBuilder(logger)
            .AddTimeout(new TimeoutStrategyOptions
            {
                Timeout = pipelineTimeout,
                OnTimeout = arg =>
                {
                    logger.LogWarning(
                        $"Message processing timed out and will be cancelled for split part. Timeout: {arg.Timeout}");

                    return ValueTask.CompletedTask;
                }
            });
    }
}