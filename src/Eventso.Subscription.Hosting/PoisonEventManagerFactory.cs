using System.Collections.Frozen;
using Eventso.Subscription.Kafka.DeadLetter;
using Eventso.Subscription.Kafka.DeadLetter.Store;

namespace Eventso.Subscription.Hosting;

//todo extract interface?
public sealed class PoisonEventManagerFactory
{
    private readonly FrozenDictionary<(string, string), PoisonEventManager> _poisonEventManagers;

    public PoisonEventManagerFactory(
        IEnumerable<ISubscriptionCollection> subscriptions,
        IPoisonEventStore poisonEventStore,
        int maxNumberOfPoisonedEventsInTopic)
    {
        _poisonEventManagers = subscriptions
            .SelectMany(x => x)
            .SelectMany(c => c.ClonePerConsumerInstance())
            .Where(c => c.EnableDeadLetterQueue)
            .ToFrozenDictionary(
                c => (c.Settings.Config.GroupId, c.SubscriptionConfigurationId),
                c => new PoisonEventManager(
                    poisonEventStore,
                    c.Settings.Config.GroupId,
                    maxNumberOfPoisonedEventsInTopic));

    }
    
    public IPoisonEventManager? Create(string groupId, string subscriptionId)
    {
        return _poisonEventManagers.TryGetValue((groupId, subscriptionId), out var poisonEventManager)
            ? poisonEventManager
            : null;
    }
}