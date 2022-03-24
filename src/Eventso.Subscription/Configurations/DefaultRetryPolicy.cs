using System;
using Microsoft.Extensions.Logging;
using Polly;

namespace Eventso.Subscription.Configurations
{
    public static class DefaultRetryPolicy
    {
        public static int FirstLevelRetryAttemptsCount = 10;
        public static int LongRetryDelayMinutes = 10;
        public static int LongRetryDurationMinutes = 3 * 60;

        public static Action<Exception, TimeSpan, int, Context, ILogger> ExceptionHandler =
            (ex, timeout, attempt, _, logger) =>
                logger.LogError(ex,
                    $"Message processing failed and will be retried. Attempt #{attempt}, timeout = {timeout}.");
        
        public static IAsyncPolicy CreateDefaultPolicy(ILogger logger)
        {
            return Policy.Handle<Exception>()
                .WaitAndRetryAsync(
                    LongRetryDurationMinutes / LongRetryDelayMinutes + FirstLevelRetryAttemptsCount,
                    retryAttempt =>
                        retryAttempt <= FirstLevelRetryAttemptsCount
                            ? TimeSpan.FromSeconds(Math.Pow(2, retryAttempt))
                            : TimeSpan.FromMinutes(LongRetryDelayMinutes),
                    (ex, timeout, attempt, context) =>
                        ExceptionHandler(ex, timeout, attempt, context, logger)
                );
        }
    }
}