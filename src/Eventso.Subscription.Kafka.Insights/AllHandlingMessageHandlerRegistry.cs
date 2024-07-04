namespace Eventso.Subscription.Kafka.Insights;

internal sealed class AllHandlingMessageHandlerRegistry : IMessageHandlersRegistry
{
    public static readonly IMessageHandlersRegistry Instance = new AllHandlingMessageHandlerRegistry();

    public bool ContainsHandlersFor(Type messageType, out HandlerKind kind)
    {
        kind = default;
        return true;
    }
}