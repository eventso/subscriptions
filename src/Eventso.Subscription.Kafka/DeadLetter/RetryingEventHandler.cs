using System;
using System.Collections.Generic;
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

            OccuredFailure? occuredFailure = null; 
            try
            {
                await _inner.Handle(@event, cancellationToken);
            }
            catch (Exception exception)
            {
                occuredFailure = new OccuredFailure(@event.GetTopicPartitionOffset(), exception.ToString());
            }

            occuredFailure ??= dlqScope.GetPoisonEvents()
                .Select(e => (OccuredFailure?)new OccuredFailure(@event.GetTopicPartitionOffset(), e.Reason))
                .SingleOrDefault();
            
            if (occuredFailure == null)
            {
                await _poisonEventStore.Remove(new [] { @event.GetTopicPartitionOffset() }, cancellationToken);
                return;
            }

            await _poisonEventStore.AddFailures(
                DateTime.UtcNow,
                new [] { occuredFailure.Value },
                cancellationToken);
        }

        public async Task Handle(IConvertibleCollection<Event> events, CancellationToken cancellationToken)
        {
            using var dlqScope = _deadLetterQueueScopeFactory.Create(events);

            OccuredFailure[] occuredFailures;
            try
            {
                await _inner.Handle(events, cancellationToken);

                occuredFailures = dlqScope.GetPoisonEvents()
                    .Select(p => new OccuredFailure(p.Event.GetTopicPartitionOffset(), p.Reason))
                    .ToArray();
            }
            catch (Exception exception) when (events.Count == 1)
            {
                occuredFailures = new[]
                {
                    new OccuredFailure(events[0].GetTopicPartitionOffset(), exception.ToString())
                };
            }

            await _poisonEventStore.AddFailures(DateTime.UtcNow, occuredFailures, cancellationToken);
            if (events.Count == occuredFailures.Length)
                return;

            var stillPoisonEventOffsets = occuredFailures.Select(e => e.TopicPartitionOffset).ToHashSet();
            await _poisonEventStore.Remove(
                events
                    .Select(h => h.GetTopicPartitionOffset())
                    .Where(e => !stillPoisonEventOffsets.Contains(e))
                    .ToArray(),
                cancellationToken);
        }
    }
}