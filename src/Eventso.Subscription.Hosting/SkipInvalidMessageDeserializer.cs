using Microsoft.Extensions.Logging.Abstractions;

namespace Eventso.Subscription.Hosting;

public class SkipInvalidMessageDeserializer : IMessageDeserializer
{
    private readonly IMessageDeserializer _inner;
    private readonly ILogger _logger;

    public SkipInvalidMessageDeserializer(IMessageDeserializer inner)
        : this(inner, NullLogger.Instance)
    {
    }

    public SkipInvalidMessageDeserializer(IMessageDeserializer inner, ILogger logger)
    {
        _inner = inner;
        _logger = logger;
    }

    public ConsumedMessage Deserialize<TContext>(ReadOnlySpan<byte> message, in TContext context)
        where TContext : IDeserializationContext
    {
        try
        {
            return _inner.Deserialize(message, context);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                $"Can't deserialize message from topic {context.Topic}. Deserializer type {_inner.GetType().Name}. Skipped.");

            return ConsumedMessage.Skipped;
        }
    }
}