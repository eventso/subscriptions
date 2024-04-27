using Eventso.Subscription.Kafka.DeadLetter;

namespace Eventso.Subscription.Hosting;

public sealed class PoisonEventQueueFactory : IPoisonEventQueueFactory
{
    private readonly Dictionary<(string, string), PoisonEventQueue> _poisonEventQueues;

    public PoisonEventQueueFactory(
        IEnumerable<ISubscriptionCollection> subscriptions,
        DeadLetterQueueOptions deadLetterQueueOptions,
        IPoisonEventStore poisonEventStore,
        IPoisonEventRetryingScheduler poisonEventRetryingScheduler)
    {
        _poisonEventQueues = subscriptions
            .SelectMany(x => x)
            .SelectMany(c => c.ClonePerConsumerInstance())
            .ToDictionary(
                c => (c.Settings.Config.GroupId, c.SubscriptionConfigurationId),
                c => new PoisonEventQueue(
                    poisonEventStore,
                    poisonEventRetryingScheduler,
                    c.Settings.Config.GroupId,
                    deadLetterQueueOptions.MaxTopicQueueSize));

    }
    
    public IPoisonEventQueue Create(string groupId, string subscriptionId)
    {
        return _poisonEventQueues.TryGetValue((groupId, subscriptionId), out var poisonEventQueue)
            ? poisonEventQueue
            : throw new Exception($"Unknown group {groupId} and subscription {subscriptionId}");
    }
}