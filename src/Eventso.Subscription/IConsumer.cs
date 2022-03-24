using System.Collections.Generic;
using System.Threading;

namespace Eventso.Subscription
{
    public interface IConsumer<T>
    {
        CancellationToken CancellationToken { get; }
        string Subscription { get; }

        void Acknowledge(in T message);
        void Acknowledge(IReadOnlyList<T> messages);

        void Cancel();
    }
}