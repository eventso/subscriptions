using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;

namespace Eventso.Subscription.Kafka.DeadLetter.Store
{
    [StructLayout(LayoutKind.Auto)]
    public readonly struct OpeningPoisonEvent
    {
        public OpeningPoisonEvent(
            PartitionOffset partitionOffset,
            Guid key,
            ReadOnlyMemory<byte> value,
            DateTime creationTimestamp,
            IReadOnlyCollection<EventHeader> headers,
            string failureReason)
        {
            PartitionOffset = partitionOffset;
            Key = key;
            Value = value;
            CreationTimestamp = creationTimestamp;
            Headers = headers;
            FailureReason = failureReason;
        }

        public PartitionOffset PartitionOffset { get; }

        public Guid Key { get; }

        public ReadOnlyMemory<byte> Value { get; }

        public DateTime CreationTimestamp { get; }

        public IReadOnlyCollection<EventHeader> Headers { get; }

        public string FailureReason { get; }
    }
}