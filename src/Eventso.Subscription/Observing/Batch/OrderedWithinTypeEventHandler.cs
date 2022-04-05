using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Eventso.Subscription.Observing.Batch
{
    public sealed class OrderedWithinTypeEventHandler<TEvent> : IEventHandler<TEvent>
        where TEvent : IEvent
    {
        private readonly IEventHandler<TEvent> _eventHandler;

        public OrderedWithinTypeEventHandler(IEventHandler<TEvent> eventHandler)
        {
            _eventHandler = eventHandler;
        }

        public Task Handle(string topic, TEvent @event, CancellationToken cancellationToken)
            => _eventHandler.Handle(topic, @event, cancellationToken);

        public async Task Handle(string topic, IConvertibleCollection<TEvent> events, CancellationToken token)
        {
            if (events.Count == 0)
                return;

            if (events.OnlyContainsSame(m => m.GetMessage().GetType()))
            {
                await _eventHandler.Handle(topic, events, token);

                return;
            }

            using var batches = OrderWithinType(events);
            foreach (var batch in batches)
                using (batch)
                    await _eventHandler.Handle(topic, batch.Events, token);
        }

        private static PooledList<BatchWithSameMessageType<TEvent>> OrderWithinType(IEnumerable<TEvent> events)
        {
            var batches = new PooledList<BatchWithSameMessageType<TEvent>>(4);

            foreach (var @event in events)
            {
                var batchFound = false;
                for (var i = 0; i < batches.Count; i++)
                {
                    if (!batches[i].TryAdd(@event))
                        continue;

                    batchFound = true;
                    break;
                }

                if (batchFound)
                    continue;

                batches.Add(new BatchWithSameMessageType<TEvent>(@event));
            }

            return batches;
        }
    }
}