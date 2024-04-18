using Eventso.Subscription.Kafka.DeadLetter;

namespace Eventso.Subscription.Hosting;

public interface IPoisonEventQueueFactory
{
    IPoisonEventQueue Create(string groupId, string subscriptionId);
}