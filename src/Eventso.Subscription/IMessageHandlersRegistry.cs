using System;

namespace Eventso.Subscription
{
    public interface IMessageHandlersRegistry
    {
        bool ContainsHandlersFor(Type messageType, out HandlerKind kind);
    }

    [Flags]
    public enum HandlerKind : byte
    {
        Single = 1,
        Batch = 2
    }
}