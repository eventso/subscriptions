using System;
using System.Runtime.InteropServices;

namespace Eventso.Subscription.Kafka.DeadLetter.Store
{
    [StructLayout(LayoutKind.Auto)]
    public readonly struct StreamId : IEquatable<StreamId>
    {
        public StreamId(string topic, Guid key)
        {
            Topic = topic;
            Key = key;
        }

        public string Topic { get; }

        public Guid Key { get; }

        public bool Equals(StreamId other)
            => Topic.Equals(other.Topic) && Key.Equals(other.Key);

        public override bool Equals(object obj)
            => obj is StreamId other && Equals(other);

        public override int GetHashCode()
            => HashCode.Combine(Topic, Key);
    }
}