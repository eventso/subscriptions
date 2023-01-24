namespace Eventso.Subscription.Kafka;

public sealed class CompositeDeserializer : IMessageDeserializer
{
    private readonly TopicDictionary<IMessageDeserializer> _topicDeserializers;

    public CompositeDeserializer(IEnumerable<(string, IMessageDeserializer)> topicDeserializers)
        => _topicDeserializers = new TopicDictionary<IMessageDeserializer>(topicDeserializers);

    public ConsumedMessage Deserialize<TContext>(ReadOnlySpan<byte> message, in TContext context)
        where TContext : IDeserializationContext
        => _topicDeserializers.Get(context.Topic).Deserialize(message, context);
}