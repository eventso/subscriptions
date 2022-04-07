using System;
using System.Collections.Generic;
using Confluent.Kafka;

namespace Eventso.Subscription.Kafka
{
    public readonly struct Event : IEvent
    {
        private readonly ConsumedMessage _value;
        private readonly Guid _key;
        internal readonly string Topic;
        internal readonly Partition Partition;
        internal readonly Offset Offset;

        public Event(ConsumeResult<Guid, ConsumedMessage> @event, string topic)
        {
            _value = @event.Message.Value;
            _key = @event.Message.Key;
            Topic = topic;
            Partition = @event.Partition;
            Offset = @event.Offset;
        }
        
        public DeserializationStatus DeserializationResult => _value.Status;

        public Guid GetKey() => _key;

        public object GetMessage() => _value.Message
                                      ?? throw new InvalidOperationException("Unknown message");

        public string GetIdentity() => $"{Topic} [{Partition}] @{Offset}";

        public IReadOnlyCollection<KeyValuePair<string, object>> GetMetadata()
        {
            var offset = new KeyValuePair<string, object>("eventso_offset", GetIdentity());
            var key = new KeyValuePair<string, object>("eventso_key", _key);

            var metadata = _value.GetMetadata();

            if (metadata?.Count > 0)
            {
                var result = new List<KeyValuePair<string, object>>(metadata.Count + 2);
                result.AddRange(metadata);
                result.Add(offset);
                result.Add(key);

                return result;
            }

            var status = DeserializationResult;
            var resultPair = new KeyValuePair<string, object>("eventso_result", status);

            if (status == DeserializationStatus.Success && _value.Message != null)
            {
                var type = new KeyValuePair<string, object>("eventso_type", _value.Message.GetType().Name);

                return new[] { offset, resultPair, type };
            }

            return new[] { offset, resultPair };
        }

        internal TopicPartitionOffset GetTopicPartitionOffset()
            => new(Topic, Partition, Offset);

    }
}