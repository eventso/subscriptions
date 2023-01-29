using Confluent.Kafka;

namespace Eventso.Subscription.Kafka.DeadLetter.Store;

public sealed class StoredPoisonEvent
{
    public StoredPoisonEvent(
        Partition partition,
        Offset offset,
        Guid key,
        ReadOnlyMemory<byte> value,
        DateTime creationTimestamp,
        IReadOnlyCollection<EventHeader> headers,
        DateTime timestamp,
        string reason,
        int totalFailureCount)
    {
        Partition = partition;
        Offset = offset;
        Key = key;
        Value = value;
        CreationTimestamp = creationTimestamp;
        Headers = headers;
        Timestamp = timestamp;
        Reason = reason;
        TotalFailureCount = totalFailureCount;
    }

    public Partition Partition { get; }

    public Offset Offset { get; }

    public Guid Key { get; }

    public ReadOnlyMemory<byte> Value { get; }

    public DateTime CreationTimestamp { get; }

    public IReadOnlyCollection<EventHeader> Headers { get; }

    public DateTime Timestamp { get; }

    public string Reason { get; }

    public int TotalFailureCount { get; }
}