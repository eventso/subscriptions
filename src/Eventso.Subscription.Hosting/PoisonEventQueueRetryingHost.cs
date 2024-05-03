namespace Eventso.Subscription.Hosting;

public sealed class PoisonEventQueueRetryingHost(
    IPoisonEventQueueRetryingService queueRetryingService,
    DeadLetterQueueOptions deadLetterQueueOptions,
    ILogger<PoisonEventQueueRetryingHost> logger)
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken token)
    {
        while (!token.IsCancellationRequested)
        {
            try
            {
                await queueRetryingService.Run(token);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, $"{nameof(PoisonEventQueueRetryingHost)} failed in {nameof(ExecuteAsync)}");
            }

            await Task.Delay(deadLetterQueueOptions.ReprocessingJobInterval, token);
        }
    }
}