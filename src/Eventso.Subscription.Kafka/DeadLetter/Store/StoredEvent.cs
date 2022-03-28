using System;
using System.Collections.Generic;
using Confluent.Kafka;

namespace Eventso.Subscription.Kafka.DeadLetter.Store
{
    public sealed class StoredEvent
    {
        public StoredEvent(
            TopicPartitionOffset topicPartitionOffset,
            Guid key,
            ReadOnlyMemory<byte> value,
            DateTime creationTimestamp,
            IReadOnlyCollection<StoredEventHeader> headers)
        {
            TopicPartitionOffset = topicPartitionOffset;
            Key = key;
            Value = value;
            CreationTimestamp = creationTimestamp;
            Headers = headers;
        }

        public TopicPartitionOffset TopicPartitionOffset { get; }

        public Guid Key { get; }

        public ReadOnlyMemory<byte> Value { get; }

        public DateTime CreationTimestamp { get; }

        public IReadOnlyCollection<StoredEventHeader> Headers { get; }
        
        
    }
}