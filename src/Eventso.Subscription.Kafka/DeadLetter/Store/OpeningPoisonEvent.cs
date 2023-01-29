using System.Runtime.InteropServices;
using Confluent.Kafka;

namespace Eventso.Subscription.Kafka.DeadLetter.Store;

[StructLayout(LayoutKind.Auto)]
public readonly struct OpeningPoisonEvent
{
    public OpeningPoisonEvent(
        TopicPartitionOffset topicPartitionOffset,
        Guid key,
        ReadOnlyMemory<byte> value,
        DateTime creationTimestamp,
        IReadOnlyCollection<EventHeader> headers,
        string failureReason)
    {
        TopicPartitionOffset = topicPartitionOffset;
        Key = key;
        Value = value;
        CreationTimestamp = creationTimestamp;
        Headers = headers;
        FailureReason = failureReason;
    }

    public TopicPartitionOffset TopicPartitionOffset { get; }

    public Guid Key { get; }

    public ReadOnlyMemory<byte> Value { get; }

    public DateTime CreationTimestamp { get; }

    public IReadOnlyCollection<EventHeader> Headers { get; }

    public string FailureReason { get; }
}