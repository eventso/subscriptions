using System;
using System.Collections.Generic;

namespace Eventso.Subscription.Kafka.DeadLetter.Store
{
    public sealed class StoredPoisonEvent
    {
        public StoredPoisonEvent(
            PartitionOffset partitionOffset,
            Guid key,
            ReadOnlyMemory<byte> value,
            DateTime creationTimestamp,
            IReadOnlyCollection<EventHeader> headers,
            DateTime timestamp,
            string reason,
            int totalFailureCount)
        {
            PartitionOffset = partitionOffset;
            Key = key;
            Value = value;
            CreationTimestamp = creationTimestamp;
            Headers = headers;
            Timestamp = timestamp;
            Reason = reason;
            TotalFailureCount = totalFailureCount;
        }

        public PartitionOffset PartitionOffset { get; }

        public Guid Key { get; }

        public ReadOnlyMemory<byte> Value { get; }

        public DateTime CreationTimestamp { get; }

        public IReadOnlyCollection<EventHeader> Headers { get; }

        public DateTime Timestamp { get; }

        public string Reason { get; }

        public int TotalFailureCount { get; }
    }
}