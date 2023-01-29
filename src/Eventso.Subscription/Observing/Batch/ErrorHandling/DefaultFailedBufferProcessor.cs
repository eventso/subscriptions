using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Eventso.Subscription.Observing.Batch.ErrorHandling
{
    internal class DefaultFailedBufferProcessor<TEvent> : IFailedBufferProcessor<TEvent>
        where TEvent : IEvent
    {
        public Task Process(
            IReadOnlyList<Buffer<TEvent>.BufferedEvent> bufferedEvents,
            CancellationToken cancellationToken)
            => Task.CompletedTask;
    }
}