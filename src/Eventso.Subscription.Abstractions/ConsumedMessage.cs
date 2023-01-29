namespace Eventso.Subscription;

public readonly struct ConsumedMessage
{
    private readonly IReadOnlyCollection<KeyValuePair<string, object>> _metadata;

    public static ConsumedMessage Skipped =>
        new ConsumedMessage(default, DeserializationStatus.Skipped, default);

    public static ConsumedMessage Unknown =>
        new ConsumedMessage(default, DeserializationStatus.UnknownType, default);

    public readonly object Message;
    public readonly DeserializationStatus Status;

    public ConsumedMessage(object message,
        DeserializationStatus status,
        IReadOnlyCollection<KeyValuePair<string, object>> metadata)
    {
        if (status == DeserializationStatus.Success && message == null)
            throw new ArgumentNullException(nameof(message),
                "Message can't be null for Success status");

        _metadata = metadata;
        Message = message;
        Status = status;
    }

    public ConsumedMessage(object message)
        : this(message, DeserializationStatus.Success, Array.Empty<KeyValuePair<string, object>>())
    {
    }

    public ConsumedMessage(object message, IReadOnlyCollection<KeyValuePair<string, object>> metadata)
        : this(message, DeserializationStatus.Success, metadata)
    {
    }

    public IReadOnlyCollection<KeyValuePair<string, object>> GetMetadata() =>
        _metadata ?? Array.Empty<KeyValuePair<string, object>>();
}