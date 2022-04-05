using System.Runtime.InteropServices;
using Confluent.Kafka;

namespace Eventso.Subscription.Kafka.DeadLetter.Store
{
    [StructLayout(LayoutKind.Auto)]
    public readonly struct PartitionOffset
    {
        public PartitionOffset(Partition partition, Offset offset)
        {
            Partition = partition;
            Offset = offset;
        }

        public Partition Partition { get; }

        public Offset Offset { get; }
    }
}