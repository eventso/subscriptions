using System.Runtime.InteropServices;

namespace Eventso.Subscription.Kafka.DeadLetter.Store
{
    [StructLayout(LayoutKind.Auto)]
    public readonly struct RecentFailure
    {
        public RecentFailure(PartitionOffset partitionOffset, string reason)
        {
            PartitionOffset = partitionOffset;
            Reason = reason;
        }

        public PartitionOffset PartitionOffset { get; }

        public string Reason { get; }
    }
}