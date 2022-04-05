using System;
using System.Collections.Generic;
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

        public async Task Handle(TEvent @event, CancellationToken cancellationToken)
        {
            if (await _poisonEventInbox.IsStreamPoisoned(@event, cancellationToken))
            {
                await _poisonEventInbox.Add(
                    new [] { new PoisonEvent<TEvent>(@event, PredecessorParkedReason) },
                    cancellationToken);
                return;
            }

            using var dlqScope = _deadLetterQueueScopeFactory.Create(@event);

            await _inner.Handle(@event, cancellationToken);

            var poisonEvents = dlqScope.GetPoisonEvents();
            if (poisonEvents.Count == 0)
                return;

            await _poisonEventInbox.Add(poisonEvents, cancellationToken);
        }

        public async Task Handle(IConvertibleCollection<TEvent> events, CancellationToken token)
        {
            var poisonStreamsEvents = await _poisonEventInbox.GetPoisonStreamsEvents(events, token);

            FilterPoisonEvents(events, poisonStreamsEvents, out var withoutPoison, out var poison);

            var healthyEvents = withoutPoison ?? events;
            using var dlqScope = _deadLetterQueueScopeFactory.Create(healthyEvents);
            await _inner.Handle(healthyEvents, token);

            var userDefinedPoison = dlqScope.GetPoisonEvents();
            if (userDefinedPoison.Count > 0)
            {
                poison ??= new PooledList<PoisonEvent<TEvent>>(userDefinedPoison.Count);
                foreach (var poisonEvent in userDefinedPoison)
                    poison.Add(poisonEvent);
            }

            if (poison?.Count > 0)
                await _poisonEventInbox.Add(poison, token);

            withoutPoison?.Dispose();
            poison?.Dispose();
        }

        private static void FilterPoisonEvents(
            IConvertibleCollection<TEvent> events,
            IReadOnlySet<TEvent> poisonStreamsEvents,
            out PooledList<TEvent> withoutPoison,
            out PooledList<PoisonEvent<TEvent>> poison)
        {
            poison = null;
            withoutPoison = null;

            if (poisonStreamsEvents.Count == 0)
                return;

            withoutPoison = new PooledList<TEvent>(events.Count - poisonStreamsEvents.Count);
            poison = new PooledList<PoisonEvent<TEvent>>(poisonStreamsEvents.Count);
            foreach (var @event in events)
            {
                if (!poisonStreamsEvents.Contains(@event))
                {
                    withoutPoison.Add(@event);
                    continue;
                }

                poison.Add(new PoisonEvent<TEvent>(@event, PredecessorParkedReason));
            }
        }
    }
}