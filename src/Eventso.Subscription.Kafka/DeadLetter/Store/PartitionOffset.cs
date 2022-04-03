using System;
using System.Runtime.InteropServices;
using Confluent.Kafka;

namespace Eventso.Subscription.Kafka.DeadLetter.Store
{
    [StructLayout(LayoutKind.Auto)]
    public readonly struct PartitionOffset : IEquatable<PartitionOffset>
    {
        public PartitionOffset(Partition partition, Offset offset)
        {
            Partition = partition;
            Offset = offset;
        }

        public Partition Partition { get; }

        public Offset Offset { get; }

        public bool Equals(PartitionOffset other)
            => Partition.Equals(other.Partition) && Offset.Equals(other.Offset);

        public override bool Equals(object obj)
            => obj is PartitionOffset other && Equals(other);

        public override int GetHashCode()
            => HashCode.Combine(Partition, Offset);
    }
}