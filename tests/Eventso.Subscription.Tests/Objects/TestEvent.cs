using System;
using System.Collections.Generic;

namespace Eventso.Subscription.Tests
{
    public readonly struct TestEvent : IEvent
    {
        private readonly Guid _key;
        private readonly object _message;

        public TestEvent(Guid key, object message, int batchNumber = 0)
        {
            BatchNumber = batchNumber;
            _key = key;
            _message = message;
        }

        public int BatchNumber { get; }

        public DeserializationStatus DeserializationResult => DeserializationStatus.Success;

        public Guid GetKey() => _key;

        public object GetMessage() => _message;

        public string GetIdentity() => Guid.NewGuid().ToString();

        public IReadOnlyCollection<KeyValuePair<string, object>> GetMetadata()
            => Array.Empty<KeyValuePair<string, object>>();
    }
}