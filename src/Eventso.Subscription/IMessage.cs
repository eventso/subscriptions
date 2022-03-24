using System;
using System.Collections.Generic;

namespace Eventso.Subscription
{
    public interface IMessage
    {
        DeserializationStatus DeserializationResult { get; }

        Guid GetKey();
        object GetPayload();

        string GetIdentity();

        IReadOnlyCollection<KeyValuePair<string, object>> GetMetadata();
    }
}