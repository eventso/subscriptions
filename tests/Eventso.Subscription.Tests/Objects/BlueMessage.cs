using System;

namespace Eventso.Subscription.Tests
{
    public sealed class BlueMessage
    {
        public Guid Id { get; }

        public BlueMessage(Guid id)
        {
            Id = id;
        }
    }
}