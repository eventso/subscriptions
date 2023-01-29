using SpanJson;

namespace Eventso.Subscription.SpanJson;

public sealed class JsonMessageDeserializer<T> : IMessageDeserializer
{
    public ConsumedMessage Deserialize<TContext>(ReadOnlySpan<byte> message, in TContext headers)
        where TContext : IDeserializationContext =>
        new(JsonSerializer.Generic.Utf8.Deserialize<T>(message));
}