using System;
using System.Collections.Generic;

namespace Eventso.Subscription.InMemory
{
    public sealed class Event : IEvent
    {
        private readonly ConsumedMessage _consumedMessage;

        public Event(ConsumedMessage consumedMessage) => _consumedMessage = consumedMessage;

        public DeserializationStatus DeserializationResult => _consumedMessage.Status;

        public bool ContainsPayload => _consumedMessage.Message != null;

        public Guid GetKey() => Guid.NewGuid();

        public object GetMessage() =>
            _consumedMessage.Message ?? throw new InvalidOperationException("Unknown message");

        public string GetIdentity() => _consumedMessage.Message.GetHashCode().ToString();

        public IReadOnlyCollection<KeyValuePair<string, object>> GetMetadata()
            => Array.Empty<KeyValuePair<string, object>>();
    }
}