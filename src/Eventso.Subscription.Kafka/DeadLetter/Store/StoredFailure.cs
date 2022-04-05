using System;
using System.Runtime.InteropServices;

namespace Eventso.Subscription.Kafka.DeadLetter.Store
{
    [StructLayout(LayoutKind.Auto)]
    public readonly struct StoredFailure
    {
        public StoredFailure(DateTime timestamp, string reason)
        {
            Timestamp = timestamp;
            Reason = reason;
        }

        public DateTime Timestamp { get; }

        public string Reason { get; }
    }
}