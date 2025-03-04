namespace Eventso.Subscription.Kafka.DeadLetter;

public interface IPoisonEventQueueFactory
{
    IPoisonEventQueue Create(string groupId, string subscriptionId);
}