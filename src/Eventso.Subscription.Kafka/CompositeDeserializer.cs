using System;
using System.Collections.Generic;

namespace Eventso.Subscription.Kafka;

public sealed class CompositeDeserializer : IMessageDeserializer
{
    private readonly Dictionary<string, IMessageDeserializer> _topicDeserializers;

    public CompositeDeserializer(Dictionary<string, IMessageDeserializer> topicDeserializers)
        => _topicDeserializers = topicDeserializers;

    public ConsumedMessage Deserialize<TContext>(ReadOnlySpan<byte> message, in TContext context)
        where TContext : IDeserializationContext
        => _topicDeserializers[context.Topic].Deserialize(message, context);
}