namespace Eventso.Subscription;

public interface IEvent
{
    DeserializationStatus DeserializationResult { get; }

    Guid GetKey();
    object GetMessage();

    string GetIdentity();

    DateTime GetUtcTimestamp();

    IReadOnlyCollection<KeyValuePair<string, object>> GetMetadata();
}
