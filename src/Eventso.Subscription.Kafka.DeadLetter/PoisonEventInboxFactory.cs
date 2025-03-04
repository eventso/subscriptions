using Microsoft.Extensions.Logging;

namespace Eventso.Subscription.Kafka.DeadLetter;

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