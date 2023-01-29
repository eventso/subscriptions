using Eventso.Subscription.Kafka.DeadLetter.Store;
using Eventso.Subscription.Observing.DeadLetter;

namespace Eventso.Subscription.Kafka.DeadLetter;

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

        string errorMessage = null;
        try
        {
            await _inner.Handle(@event, cancellationToken);

            var poisonEvents = dlqScope.GetPoisonEvents();
            if (poisonEvents.Count > 0)
                errorMessage = poisonEvents.Single().Reason;
        }
        catch (Exception exception)
        {
            errorMessage = exception.ToString();
        }

        if (errorMessage != null)
        {
            await _poisonEventStore.AddFailure(
                DateTime.UtcNow,
                new OccuredFailure(@event.GetTopicPartitionOffset(), errorMessage),
                cancellationToken);
            return;
        }

        await _poisonEventStore.Remove(@event.GetTopicPartitionOffset(), cancellationToken);
    }

    public async Task Handle(IConvertibleCollection<Event> events, CancellationToken cancellationToken)
    {
        using var dlqScope = _deadLetterQueueScopeFactory.Create(events);

        var occuredFailures = Array.Empty<OccuredFailure>();
        try
        {
            await _inner.Handle(events, cancellationToken);

            var poisonEvents = dlqScope.GetPoisonEvents();
            if (poisonEvents.Count > 0)
                occuredFailures = poisonEvents
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

        if (occuredFailures.Length > 0)
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