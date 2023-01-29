using System.Runtime.InteropServices;
using Confluent.Kafka;

namespace Eventso.Subscription.Kafka.DeadLetter.Store;

[StructLayout(LayoutKind.Auto)]
public readonly struct OccuredFailure
{
    public OccuredFailure(TopicPartitionOffset topicPartitionOffset, string reason)
    {
        TopicPartitionOffset = topicPartitionOffset;
        Reason = reason;
    }

    public TopicPartitionOffset TopicPartitionOffset { get; }

    public string Reason { get; }
}