using Eventso.Subscription.Kafka.DeadLetter.Store;
using Eventso.Subscription.Observing.DeadLetter;

namespace Eventso.Subscription.Kafka.DeadLetter;

public sealed class PoisonStreamCollection : IPoisonStreamCollection<Event>
{
    private readonly HashSet<StreamId> _poisonStreamIds;

    public PoisonStreamCollection(HashSet<StreamId> poisonStreamIds)
        => _poisonStreamIds = poisonStreamIds;

    public bool IsPartOfPoisonStream(Event @event)
        => _poisonStreamIds.Contains(new StreamId(@event.Topic, @event.GetKey()));
}