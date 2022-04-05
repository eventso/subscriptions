using System;
using System.Collections.Generic;
using System.Linq;
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
            if (await _poisonEventInbox.Contains(topic, @event.GetKey(), cancellationToken))
            {
                await _poisonEventInbox.Add(
                    new [] { new PoisonEvent<TEvent>(topic, @event, PredecessorParkedReason) },
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
            var poisonKeys = await _poisonEventInbox.GetContainedKeys(
                topic,
                events.Convert(e => e.GetKey()),
                token);

            FilterPoisonEvents(topic, events, poisonKeys, out var withoutPoison, out var poison);

            var healthyEvents = withoutPoison ?? events;
            using var dlqScope = _deadLetterQueueScopeFactory.Create(topic, healthyEvents);
            await _inner.Handle(topic, healthyEvents, token);

            var userDefinedPoison = dlqScope.GetPoisonEvents();
            if (userDefinedPoison.Count > 0)
            {
                poison ??= new PooledList<PoisonEvent<TEvent>>(userDefinedPoison.Count);
                foreach (var poisonEvent in userDefinedPoison)
                    poison.Add(poisonEvent);
            }

            await _poisonEventInbox.Add(poison, token);

            withoutPoison?.Dispose();
            poison?.Dispose();
        }

        private static void FilterPoisonEvents(
            string topic,
            IConvertibleCollection<TEvent> events,
            IReadOnlySet<Guid> poisonKeys,
            out PooledList<TEvent> withoutPoison,
            out PooledList<PoisonEvent<TEvent>> poison)
        {
            poison = null;
            withoutPoison = null;

            if (poisonKeys.Count == 0)
                return;

            for (var i = 0; i < events.Count; i++)
            {
                var @event = events[i];
                if (!poisonKeys.Contains(@event.GetKey()))
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
        }
    }
}