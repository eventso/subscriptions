using System;
using System.Threading;
using System.Threading.Tasks;

namespace Eventso.Subscription.Observing.DeadLetter
{
    public sealed class PoisonEventHandler<TEvent> : IEventHandler<TEvent>
        where TEvent : IEvent
    {
        internal const string PoisonPredecessorReason = "Predecessor of event is poison.";

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
            if (await _poisonEventInbox.IsPartOfPoisonStream(@event, cancellationToken))
            {
                await _poisonEventInbox.Add(
                    new PoisonEvent<TEvent>(@event, PoisonPredecessorReason),
                    cancellationToken);
                return;
            }

            using var dlqScope = _deadLetterQueueScopeFactory.Create(@event);

            PoisonEvent<TEvent>? poisonEvent = null;
            try
            {
                await _inner.Handle(@event, cancellationToken);
            }
            catch (Exception exception)
            {
                poisonEvent = new PoisonEvent<TEvent>(@event, exception.ToString());
            }

            var poisonEvents = poisonEvent != null
                ? new[] { poisonEvent.Value }
                : dlqScope.GetPoisonEvents();
            if (poisonEvents.Count == 0)
                return;

            await _poisonEventInbox.Add(poisonEvents, cancellationToken);
        }

        public async Task Handle(IConvertibleCollection<TEvent> events, CancellationToken token)
        {
            var poisonStreamCollection = await _poisonEventInbox.GetPoisonStreams(events, token);

            FilterPoisonEvents(events, poisonStreamCollection, out var withoutPoison, out var poison);

            var healthyEvents = withoutPoison ?? events;
            using var dlqScope = _deadLetterQueueScopeFactory.Create(healthyEvents);

            try
            {
                await _inner.Handle(healthyEvents, token);

                var userDefinedPoison = dlqScope.GetPoisonEvents();
                if (userDefinedPoison.Count > 0)
                {
                    poison ??= new PooledList<PoisonEvent<TEvent>>(userDefinedPoison.Count);
                    foreach (var poisonEvent in userDefinedPoison)
                        poison.Add(poisonEvent);
                }
            }
            catch (Exception exception) when (healthyEvents.Count == 1)
            {
                poison ??= new PooledList<PoisonEvent<TEvent>>(1);
                poison.Add(new PoisonEvent<TEvent>(healthyEvents[0], exception.ToString()));
            }

            if (poison?.Count > 0)
                await _poisonEventInbox.Add(poison, token);

            withoutPoison?.Dispose();
            poison?.Dispose();
        }

        private static void FilterPoisonEvents(
            IConvertibleCollection<TEvent> events,
            IPoisonStreamCollection<TEvent> poisonStreamCollection,
            out PooledList<TEvent> withoutPoison,
            out PooledList<PoisonEvent<TEvent>> poison)
        {
            poison = null;
            withoutPoison = null;

            if (poisonStreamCollection == null)
                return;

            withoutPoison = new PooledList<TEvent>(events.Count - 1);
            poison = new PooledList<PoisonEvent<TEvent>>(1);
            foreach (var @event in events)
            {
                if (!poisonStreamCollection.IsPartOfPoisonStream(@event))
                {
                    withoutPoison.Add(@event);
                    continue;
                }

                poison.Add(new PoisonEvent<TEvent>(@event, PoisonPredecessorReason));
            }
        }
    }
}