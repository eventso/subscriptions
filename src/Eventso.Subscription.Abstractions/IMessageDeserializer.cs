namespace Eventso.Subscription;

public interface IMessageDeserializer
{
    /// <summary>
    /// Note: Deserializer can check handler registration via context.IsHandlerRegisteredFor
    /// and return ConsumedMessage.Skipped in case of absent handler for the message type.  
    /// </summary>
    ConsumedMessage Deserialize<TContext>(ReadOnlySpan<byte> message, in TContext context)
        where TContext : IDeserializationContext;
}