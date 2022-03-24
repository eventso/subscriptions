using System;

namespace Eventso.Subscription.Inbox
{
    public static class StreamIdExtensions
    {
        public static long GetLockId(this Guid streamId)
        {
            Span<byte> streamIdBytes = stackalloc byte[16];
            if (!streamId.TryWriteBytes(streamIdBytes))
                throw new InvalidOperationException("Can't write bytes");

            return BitConverter.ToInt64(streamIdBytes.Slice(0, 8))
                   ^ BitConverter.ToInt64(streamIdBytes.Slice(8))
                   ^ long.MaxValue;
        }
    }
}