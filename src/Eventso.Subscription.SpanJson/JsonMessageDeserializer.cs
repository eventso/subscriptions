using SpanJson;

namespace Eventso.Subscription.SpanJson;

public sealed class JsonMessageDeserializer<T> : IMessageDeserializer
{
    private readonly bool _skipEmpty;

    public JsonMessageDeserializer(bool skipEmpty = false)
        => _skipEmpty = skipEmpty;

    public ConsumedMessage Deserialize<TContext>(ReadOnlySpan<byte> message, in TContext headers)
        where TContext : IDeserializationContext
    {
        return _skipEmpty && message.IsEmpty
            ? ConsumedMessage.Skipped
            : new ConsumedMessage(JsonSerializer.Generic.Utf8.Deserialize<T>(message));
    }
}