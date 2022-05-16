using System;
using System.Runtime.InteropServices;

namespace Eventso.Subscription.Kafka.DeadLetter.Store
{
    [StructLayout(LayoutKind.Auto)]
    public readonly struct EventHeader
    {
        public EventHeader(string key, ReadOnlyMemory<byte> data)
        {
            Key = key;
            Data = data;
        }

        public string Key { get; }

        public ReadOnlyMemory<byte> Data { get; }
    }
}