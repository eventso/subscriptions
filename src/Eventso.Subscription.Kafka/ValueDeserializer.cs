using Confluent.Kafka;

namespace Eventso.Subscription.Kafka;

public sealed class ValueDeserializer : IDeserializer<ConsumedMessage>
{
    private readonly IMessageDeserializer _deserializer;
    private readonly IMessageHandlersRegistry _registry;

    public ValueDeserializer(
        IMessageDeserializer deserializer,
        IMessageHandlersRegistry registry)
    {
        _deserializer = deserializer;
        _registry = registry;
    }

    public ConsumedMessage Deserialize(
        ReadOnlySpan<byte> data,
        bool isNull,
        SerializationContext context)
    {
        try
        {
            var internalContext = new DeserializationContext(context.Topic, context.Headers, _registry);

            return _deserializer.Deserialize(data, internalContext);
        }
        catch (Exception ex)
        {
            throw new InvalidEventException(
                context.Topic,
                $"Can't deserialize message. Deserializer type {_deserializer.GetType().Name}",
                ex);
        }
    }

    private readonly struct DeserializationContext : IDeserializationContext
    {
        private readonly Headers _headers;
        private readonly IMessageHandlersRegistry _registry;

        public DeserializationContext(string topic, Headers headers, IMessageHandlersRegistry registry)
        {
            Topic = topic;
            _headers = headers;
            _registry = registry;
        }

        public int HeadersCount => _headers.Count;

        public string Topic { get; }

        public (string name, byte[] value) GetHeader(int index)
        {
            var header = _headers[index];
            return (header.Key, header.GetValueBytes());
        }

        public byte[] GetHeaderValue(string name) => _headers.GetLastBytes(name);

        public bool IsHandlerRegisteredFor(Type messageType) =>
            _registry.ContainsHandlersFor(messageType, out _);
    }
}