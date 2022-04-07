using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Eventso.Subscription.Kafka.DeadLetter.Store;
using Eventso.Subscription.Observing.DeadLetter;

namespace Eventso.Subscription.Kafka.DeadLetter
{
    public sealed class RetryingEventHandler : IEventHandler<Event>
    {
        private readonly IEventHandler<Event> _inner;
        private readonly IDeadLetterQueueScopeFactory _deadLetterQueueScopeFactory;
        private readonly IPoisonEventStore _poisonEventStore;

        public RetryingEventHandler(
            IEventHandler<Event> inner,
            IDeadLetterQueueScopeFactory deadLetterQueueScopeFactory,
            IPoisonEventStore poisonEventStore)
        {
            _inner = inner;
            _deadLetterQueueScopeFactory = deadLetterQueueScopeFactory;
            _poisonEventStore = poisonEventStore;
        }

        public async Task Handle(Event @event, CancellationToken cancellationToken)
        {
            using var dlqScope = _deadLetterQueueScopeFactory.Create(@event);

            await _inner.Handle(@event, cancellationToken);

            var poisonEvents = dlqScope.GetPoisonEvents();
            if (poisonEvents.Count == 0)
            {
                await _poisonEventStore.Remove(new [] { @event.GetTopicPartitionOffset() }, cancellationToken);
                return;
            }

            await _poisonEventStore.AddFailures(
                DateTime.UtcNow,
                new [] { new OccuredFailure(@event.GetTopicPartitionOffset(), poisonEvents.Single().Reason) },
                cancellationToken);
        }

        public async Task Handle(IConvertibleCollection<Event> events, CancellationToken cancellationToken)
        {
            using var dlqScope = _deadLetterQueueScopeFactory.Create(events);

            await _inner.Handle(events, cancellationToken);

            var stillPoisonEvents = dlqScope.GetPoisonEvents();

            await _poisonEventStore.AddFailures(
                DateTime.UtcNow,
                stillPoisonEvents.Select(p => new OccuredFailure(p.Event.GetTopicPartitionOffset(), p.Reason)).ToArray(),
                cancellationToken);

            var stillPoisonEventOffsets = stillPoisonEvents.Select(e => e.Event.GetTopicPartitionOffset()).ToHashSet();
            await _poisonEventStore.Remove(
                events
                    .Select(h => h.GetTopicPartitionOffset())
                    .Where(e => !stillPoisonEventOffsets.Contains(e))
                    .ToArray(),
                cancellationToken);
        }
    }
}