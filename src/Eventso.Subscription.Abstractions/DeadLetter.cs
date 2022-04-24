using System;
using System.Collections.Generic;

namespace Eventso.Subscription
{
    public readonly struct DeadLetter : IEquatable<DeadLetter>
    {
        public DeadLetter(object message, string reason)
        {
            Message = message;
            Reason = reason;
        }

        public object Message { get; }

        public string Reason { get; }

        public bool Equals(DeadLetter other)
            => MessageComparer.Equals(Message, other.Message) && Reason == other.Reason;

        public override bool Equals(object obj)
            => obj is DeadLetter other && Equals(other);

        public override int GetHashCode()
            => HashCode.Combine(Message, Reason);

        public static IEqualityComparer<object> MessageComparer
            => ReferenceEqualityComparer.Instance;
    }
}
