namespace Eventso.Subscription.Tests;

public abstract class MockEvent : IEvent
{
    public abstract DeserializationStatus DeserializationResult { get; }
    public abstract Guid GetKey();
    public abstract object GetMessage();
    public abstract string GetIdentity();
    public abstract IReadOnlyCollection<KeyValuePair<string, object>> GetMetadata();
    public abstract DateTime GetUtcTimestamp();
}
