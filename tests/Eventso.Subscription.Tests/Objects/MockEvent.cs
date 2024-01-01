namespace Eventso.Subscription.Tests;

public abstract class MockEvent : IEvent, IGroupedMetadata<MockEvent>
{
    public abstract DeserializationStatus DeserializationResult { get; }
    public abstract Guid GetKey();
    public abstract object GetMessage();
    public abstract string GetIdentity();
    public abstract IReadOnlyCollection<KeyValuePair<string, object>> GetMetadata();
    public abstract DateTime GetUtcTimestamp();

    public static IEnumerable<KeyValuePair<string, object>[]> GroupedMetadata(IEnumerable<MockEvent> items)
        => Array.Empty<KeyValuePair<string, object>[]>();
}
