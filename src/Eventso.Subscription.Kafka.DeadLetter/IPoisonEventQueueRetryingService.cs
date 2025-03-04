namespace Eventso.Subscription.Kafka.DeadLetter;

public interface IPoisonEventQueueRetryingService
{
    Task Run(CancellationToken token);
}