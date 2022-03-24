using System;
using System.Collections.Generic;

namespace Eventso.Subscription.InMemory
{
    public sealed class Message : IMessage
    {
        private readonly ConsumedMessage _consumedMessage;

        public Message(ConsumedMessage consumedMessage) => _consumedMessage = consumedMessage;

        public DeserializationStatus DeserializationResult => _consumedMessage.Status;

        public bool ContainsPayload => _consumedMessage.Message != null;

        public Guid GetKey() => Guid.NewGuid();

        public object GetPayload() =>
            _consumedMessage.Message ?? throw new InvalidOperationException("Unknown message");

        public string GetIdentity() => _consumedMessage.Message.GetHashCode().ToString();

        public IReadOnlyCollection<KeyValuePair<string, object>> GetMetadata()
            => Array.Empty<KeyValuePair<string, object>>();
    }
}