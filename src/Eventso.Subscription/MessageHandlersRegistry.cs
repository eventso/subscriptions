using System.Collections.Concurrent;

namespace Eventso.Subscription;

public class MessageHandlersRegistry(IServiceProviderIsService isServiceProvider) : IMessageHandlersRegistry
{
    private readonly ConcurrentDictionary<Type, HandlerKind> _lookup = [];

    public bool ContainsHandlersFor(Type messageType, out HandlerKind kind)
    {
        kind = _lookup.GetOrAdd(messageType, Resolve, isServiceProvider);

        return kind != default;

        static HandlerKind Resolve(Type messageType, IServiceProviderIsService isService)
        {
            HandlerKind result = default;

            var singleHandlerType = typeof(IMessageHandler<>).MakeGenericType(messageType);
            if (isService.IsService(singleHandlerType))
                result |= HandlerKind.Single;

            var batchHandlerType = typeof(IMessageHandler<>).MakeGenericType(typeof(IReadOnlyCollection<>).MakeGenericType(messageType));
            if (isService.IsService(batchHandlerType))
                result |= HandlerKind.Batch;

            return result;
        }
    }
}
