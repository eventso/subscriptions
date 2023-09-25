namespace Eventso.Subscription.Tests;

public readonly struct TestEvent : IEvent, IEquatable<TestEvent>
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

    public Guid Key => _key; // for test output

    public Guid GetKey() => _key;

    public object GetMessage() => _message;

    public string GetIdentity() => Guid.NewGuid().ToString();

    public DateTime GetUtcTimestamp()
        => DateTime.UtcNow;

    public IReadOnlyCollection<KeyValuePair<string, object>> GetMetadata()
        => Array.Empty<KeyValuePair<string, object>>();

    public bool Equals(TestEvent other)
        => _key.Equals(other._key) && Equals(_message, other._message) && BatchNumber == other.BatchNumber;

    public override bool Equals(object? obj)
        => obj is TestEvent other && Equals(other);

    public override int GetHashCode()
        => HashCode.Combine(_key, _message, BatchNumber);
}