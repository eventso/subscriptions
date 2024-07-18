namespace Eventso.Subscription.Observing.Batch;

public sealed class OrderedWithinTypeEventHandler<TEvent> : IEventHandler<TEvent>
    where TEvent : IEvent
{
    private readonly IEventHandler<TEvent> _nextHandler;

    public OrderedWithinTypeEventHandler(IEventHandler<TEvent> nextHandler)
    {
        _nextHandler = nextHandler;
    }

    public Task Handle(TEvent @event, HandlingContext context, CancellationToken cancellationToken)
        => _nextHandler.Handle(@event, context, cancellationToken);

    public async Task Handle(IConvertibleCollection<TEvent> events, HandlingContext context, CancellationToken token)
    {
        if (events.Count == 0)
            return;

        if (events.OnlyContainsSame(m => m.GetMessage().GetType()))
        {
            await _nextHandler.Handle(events, context, token);

            return;
        }

        using var batches = OrderWithinType(events);
        foreach (var batch in batches)
            using (batch)
                await _nextHandler.Handle(batch.Events, context, token);
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