using System;
using System.Collections.Generic;

namespace Eventso.Subscription
{
    public interface IEvent
    {
        DeserializationStatus DeserializationResult { get; }

        Guid GetKey();
        object GetMessage();

        string GetIdentity();

        IReadOnlyCollection<KeyValuePair<string, object>> GetMetadata();
    }
}