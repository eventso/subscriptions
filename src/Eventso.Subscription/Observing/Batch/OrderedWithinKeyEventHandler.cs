namespace Eventso.Subscription.Observing.Batch;

public sealed class OrderedWithinKeyEventHandler<TEvent> : IEventHandler<TEvent>
    where TEvent : IEvent
{
    private readonly IEventHandler<TEvent> _nextHandler;

    public OrderedWithinKeyEventHandler(IEventHandler<TEvent> nextHandler)
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

        using var batches = OrderWithinKey(events);
        foreach (var batch in batches)
            using (batch)
                await _nextHandler.Handle(batch.Events, context, token);
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