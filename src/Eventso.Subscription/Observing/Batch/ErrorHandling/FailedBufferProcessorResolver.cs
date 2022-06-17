using System;
using Eventso.Subscription.Configurations;

namespace Eventso.Subscription.Observing.Batch.ErrorHandling
{
    internal static class FailedBufferProcessorResolver
    {
        internal static IFailedBufferProcessor<TEvent> Resolve<TEvent>(
            FailedBatchProcessingStrategy failedBatchProcessingStrategy,
            IEventHandler<TEvent> handler,
            IConsumer<TEvent> consumer)
            where TEvent : IEvent
            => failedBatchProcessingStrategy switch
            {
                FailedBatchProcessingStrategy.None => new DefaultFailedBufferProcessor<TEvent>(),
                FailedBatchProcessingStrategy.Breakdown => new BreakdownFailedBufferProcessor<TEvent>(handler, consumer),
                _ => throw new ArgumentOutOfRangeException(
                    nameof(failedBatchProcessingStrategy),
                    failedBatchProcessingStrategy,
                    null)
            };
    }
}