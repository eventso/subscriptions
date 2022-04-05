using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Eventso.Subscription.Observing.Batch
{
    public sealed class OrderedWithinKeyEventHandler<TEvent> : IEventHandler<TEvent>
        where TEvent : IEvent
    {
        private readonly IEventHandler<TEvent> _eventHandler;

        public OrderedWithinKeyEventHandler(IEventHandler<TEvent> eventHandler)
        {
            _eventHandler = eventHandler;
        }

        public Task Handle(TEvent @event, CancellationToken cancellationToken)
            => _eventHandler.Handle(@event, cancellationToken);

        public async Task Handle(IConvertibleCollection<TEvent> events, CancellationToken token)
        {
            if (events.Count == 0)
                return;

            if (events.OnlyContainsSame(m => m.GetMessage().GetType()))
            {
                await _eventHandler.Handle(events, token);
                return;
            }

            using var batches = OrderWithinKey(events);
            foreach (var batch in batches)
                using (batch)
                    await _eventHandler.Handle(batch.Events, token);
        }

        private static PooledList<BatchWithSameMessageType<TEvent>> OrderWithinKey(IEnumerable<TEvent> events)
        {
            var groupedEvents = events.GroupBy(m => m.GetKey());
            var batches = new PooledList<BatchWithSameMessageType<TEvent>>(4);

            foreach (var group in groupedEvents)
            {
                var batchIndex = 0;

                foreach (var @event in group)
                {
                    while (true)
                    {
                        if (batchIndex == batches.Count)
                        {
                            batches.Add(new BatchWithSameMessageType<TEvent>(@event));
                            break;
                        }

                        var currentBatch = batches[batchIndex];

                        if (currentBatch.TryAdd(@event))
                            break;

                        ++batchIndex;
                    }
                }
            }

            return batches;
        }
    }
}