using System.Collections.Frozen;

namespace Eventso.Subscription.Kafka;

public sealed class CompositeDeserializer : IMessageDeserializer
{
    private readonly FrozenDictionary<string,IMessageDeserializer> _topicDeserializers;

    public CompositeDeserializer(IEnumerable<KeyValuePair<string, IMessageDeserializer>> topicDeserializers)
        => _topicDeserializers = topicDeserializers.ToFrozenDictionary();

    public ConsumedMessage Deserialize<TContext>(ReadOnlySpan<byte> message, in TContext context)
        where TContext : IDeserializationContext
        => _topicDeserializers[context.Topic].Deserialize(message, context);
}