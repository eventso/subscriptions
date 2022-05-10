using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Eventso.Subscription.Observing.Batch.ErrorHandling
{
    internal interface IFailedBufferProcessor<TEvent>
        where TEvent : IEvent
    {
        Task Process(IReadOnlyList<Buffer<TEvent>.BufferedEvent> bufferedEvents, CancellationToken cancellationToken);
    }
}