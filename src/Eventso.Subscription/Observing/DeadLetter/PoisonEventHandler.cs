using System.Threading;
using System.Threading.Tasks;

namespace Eventso.Subscription.Observing.DeadLetter
{
    public sealed class PoisonEventHandler<TEvent> : IEventHandler<TEvent>
        where TEvent : IEvent
    {
        private const string PredecessorParkedReason = "Predecessor of event is poison and parked.";

        private readonly IPoisonEventInbox<TEvent> _poisonEventInbox;
        private readonly IDeadLetterQueueScopeFactory _deadLetterQueueScopeFactory;
        private readonly IEventHandler<TEvent> _inner;

        public PoisonEventHandler(
            IPoisonEventInbox<TEvent> poisonEventInbox,
            IDeadLetterQueueScopeFactory deadLetterQueueScopeFactory,
            IEventHandler<TEvent> inner)
        {
            _poisonEventInbox = poisonEventInbox;
            _deadLetterQueueScopeFactory = deadLetterQueueScopeFactory;
            _inner = inner;
        }

        public async Task Handle(string topic, TEvent @event, CancellationToken cancellationToken)
        {
            if (await _poisonEventInbox.IsPredecessorAdded(topic, @event.GetKey(), cancellationToken))
            {
                await _poisonEventInbox.Add(
                    new PoisonEvent<TEvent>(topic, @event, PredecessorParkedReason),
                    cancellationToken);
                return;
            }

            using var dlqScope = _deadLetterQueueScopeFactory.Create(topic, @event);

            await _inner.Handle(topic, @event, cancellationToken);

            var poisonEvents = dlqScope.GetPoisonEvents();
            if (poisonEvents.Count == 0)
                return;

            await _poisonEventInbox.Add(poisonEvents, cancellationToken);
        }

        public async Task Handle(string topic, IConvertibleCollection<TEvent> events, CancellationToken token)
        {
            // TODO make it bulk
            PooledList<TEvent> withoutPoison = null;
            PooledList<PoisonEvent<TEvent>> poison = null;
            for (var i = 0; i < events.Count; i++)
            {
                var @event = events[i];
                if (!await _poisonEventInbox.IsPredecessorAdded(topic, @event.GetKey(), token))
                    continue;

                if (withoutPoison == null)
                {
                    withoutPoison = new PooledList<TEvent>(events.Count - 1);
                    for (var j = 0; j < i; j++)
                        withoutPoison.Add(events[j]);
                }

                withoutPoison.Add(@event);

                poison ??= new PooledList<PoisonEvent<TEvent>>(1);
                poison.Add(new PoisonEvent<TEvent>(topic, @event, PredecessorParkedReason));
            }

            var leftEvents = withoutPoison ?? events;
            using var dlqScope = _deadLetterQueueScopeFactory.Create(topic, leftEvents);

            await _inner.Handle(topic, withoutPoison ?? events, token);

            var poisonEvents = dlqScope.GetPoisonEvents();
            if (poisonEvents.Count > 0)
            {
                poison ??= new PooledList<PoisonEvent<TEvent>>(poisonEvents.Count);
                foreach (var poisonEvent in poisonEvents)
                    poison.Add(poisonEvent);
                await _poisonEventInbox.Add(poison, token);
            }

            withoutPoison?.Dispose();
        }
    }
}