using System.Runtime.InteropServices;
using Confluent.Kafka;

namespace Eventso.Subscription.Kafka.DeadLetter.Store;

[StructLayout(LayoutKind.Auto)]
public sealed record PoisonEvent(
    TopicPartitionOffset TopicPartitionOffset,
    ReadOnlyMemory<byte> Key,
    ReadOnlyMemory<byte> Value,
    DateTime CreationTimestamp,
    IReadOnlyCollection<EventHeader> Headers,
    int FailureCount)
{
    public static PoisonEvent From(ConsumeResult<byte[], byte[]> poisonMessage)
    {
        return new PoisonEvent(
            poisonMessage.TopicPartitionOffset,
            poisonMessage.Message.Key,
            poisonMessage.Message.Value,
            poisonMessage.Message.Timestamp.UtcDateTime,
            poisonMessage.Message
                .Headers
                .Select(c => new EventHeader(c.Key, c.GetValueBytes()))
                .ToArray(),
            1);
    }
}