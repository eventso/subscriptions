using System;

namespace Eventso.Subscription.Tests
{
    public sealed class BlueEvent
    {
        public Guid Id { get; }

        public BlueEvent(Guid id)
        {
            Id = id;
        }
    }
}