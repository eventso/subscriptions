using Eventso.Subscription.Kafka;
using Eventso.Subscription.Kafka.DeadLetter;
using Eventso.Subscription.Observing.DeadLetter;

namespace Eventso.Subscription.Hosting.DeadLetter;

public sealed class PoisonEventInboxFactory(
    IPoisonEventQueue poisonEventQueue,
    KafkaConsumerSettings settings,
    ILoggerFactory loggerFactory) : IPoisonEventInboxFactory<Event>
{
    public IPoisonEventInbox<Event> Create(string topic)
    {
        return new PoisonEventInbox(
            poisonEventQueue,
            settings,
            topic,
            loggerFactory.CreateLogger<PoisonEventInbox>());
    }
}