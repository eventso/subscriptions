namespace Eventso.Subscription.Hosting;

public interface IPoisonEventQueueRetryingService
{
    Task Run(CancellationToken token);
}