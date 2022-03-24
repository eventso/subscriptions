using System;

namespace Eventso.Subscription.Inbox
{
    public readonly struct EventRecord
    {
        public Guid StreamId { get; }
        public long Number { get; }
        public byte[] Body { get; }
        public string Error { get; }

        public EventRecord(Guid streamId,
            long number,
            byte[] body,
            string error = "")
        {
            StreamId = streamId;
            Number = number;
            Body = body;
            Error = error;
        }
    }
}