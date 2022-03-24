using System;
using System.Collections.Generic;

namespace Eventso.Subscription.Tests
{
    public readonly struct TestMessage : IMessage
    {
        private readonly Guid _key;
        private readonly object _payload;

        public TestMessage(Guid key, object payload, int batchNumber = 0)
        {
            BatchNumber = batchNumber;
            _key = key;
            _payload = payload;
        }

        public int BatchNumber { get; }

        public DeserializationStatus DeserializationResult => DeserializationStatus.Success;

        public Guid GetKey() => _key;

        public object GetPayload() => _payload;

        public string GetIdentity() => Guid.NewGuid().ToString();

        public IReadOnlyCollection<KeyValuePair<string, object>> GetMetadata()
            => Array.Empty<KeyValuePair<string, object>>();
    }
}