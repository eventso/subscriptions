namespace Eventso.Subscription.Hosting.DeadLetter;

public interface IPoisonEventQueueRetryingService
{
    Task Run(CancellationToken token);
}