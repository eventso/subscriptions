namespace Eventso.Subscription.Kafka.DeadLetter.Store
{
    public sealed class StoredPoisonEvent
    {
        public StoredEvent Event { get; }
        public StoredFailure LastFailure { get; }
        public int TotalFailureCount { get; }

        public StoredPoisonEvent(
            StoredEvent @event,
            StoredFailure lastFailure,
            int totalFailureCount)
        {
            Event = @event;
            LastFailure = lastFailure;
            TotalFailureCount = totalFailureCount;
        }
    }
}