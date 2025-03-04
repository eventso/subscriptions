using System.Runtime.InteropServices;
using Confluent.Kafka;

namespace Eventso.Subscription.Kafka.DeadLetter;

public sealed record PoisonEvent(
    TopicPartitionOffset TopicPartitionOffset,
    string GroupId,
    ReadOnlyMemory<byte> Key,
    ReadOnlyMemory<byte> Value,
    DateTime CreationTimestamp,
    IReadOnlyCollection<PoisonEvent.Header> Headers,
    DateTime LastFailureTimestamp,
    string LastFailureReason,
    int TotalFailureCount,
    DateTime? LockTimestamp,
    DateTime UpdateTimestamp)
{
    [StructLayout(LayoutKind.Auto)]
    public readonly record struct Header(string Key, ReadOnlyMemory<byte> Data);
}