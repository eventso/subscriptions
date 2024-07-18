namespace Eventso.Subscription.Observing.DeadLetter;

public sealed class PoisonEventHandler<TEvent>(
    IPoisonEventInbox<TEvent> poisonEventInbox,
    IEventHandler<TEvent> inner,
    ILogger<PoisonEventHandler<TEvent>> logger)
    : IEventHandler<TEvent>
    where TEvent : IEvent
{
    internal const string StreamIsPoisonReason = "Event stream is poisoned.";
    
    public async Task Handle(TEvent @event, HandlingContext context, CancellationToken token)
    {
        await HandleSingle(@event, e => e, inner.Handle, context, token);
    }

    public async Task Handle(IConvertibleCollection<TEvent> events, HandlingContext context, CancellationToken token)
    {
        if (events.Count == 0)
            return;
        
        try
        {
            await HandleBatch(events, context, token);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception) when (events.Count > 1)
        {
            // todo this unwrapping should be binary search like
            logger.UnwrapStarted(events.Count);

            for (var i = 1; i <= events.Count; i++)
            {
                using var pooledList = new PooledList<TEvent>(1); // expensive, but...
                pooledList.Add(events[i - 1]);
                await HandleSingle(pooledList, e => e[0], inner.Handle, context, token);

                if (i % 200 == 0)
                    logger.UnwrapInProgress(i, events.Count);
            }

            logger.UnwrapCompleted(events.Count);
        }
    }

    private async Task HandleSingle<THandleParam>(
        THandleParam singleEventContainer, // bit weird but its private
        Func<THandleParam, TEvent> getEvent,
        Func<THandleParam, HandlingContext, CancellationToken, Task> handle,
        HandlingContext context,
        CancellationToken token)
    {
        var @event = getEvent(singleEventContainer);
        
        if (await poisonEventInbox.IsPartOfPoisonStream(@event, token))
        {
            await poisonEventInbox.Add(@event, StreamIsPoisonReason, token);
            return;
        }

        try
        {
            await handle(singleEventContainer, context, token);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            await poisonEventInbox.Add(@event, exception.ToString(), token);
        }
    }

    private async Task HandleBatch(IConvertibleCollection<TEvent> events, HandlingContext context, CancellationToken token)
    {
        if (events.Count <= 1)
            throw new ArgumentException("Expected more than 1 element in events collection", nameof(events));
        
        var poisonStreamCollection = await GetPoisonStreams(events, token);

        FilterPoisonEvents(events, poisonStreamCollection, out var withoutPoison, out var poison);

        var healthyEvents = withoutPoison ?? events;

        await inner.Handle(healthyEvents, context, token);

        if (poison?.Count > 0)
            foreach (var @event in poison)
                await poisonEventInbox.Add(@event.Event, @event.Reason, token);

        withoutPoison?.Dispose();
        poison?.Dispose();
    }

    private async ValueTask<HashSet<TEvent>?> GetPoisonStreams(
        IConvertibleCollection<TEvent> events,
        CancellationToken token)
    {
        HashSet<TEvent>? poisonEvents = null;
        foreach (var @event in events)
        {
            if (!await poisonEventInbox.IsPartOfPoisonStream(@event, token))
                continue;

            poisonEvents ??= new HashSet<TEvent>();
            poisonEvents.Add(@event);
        }

        return poisonEvents;
    }

    private static void FilterPoisonEvents(
        IConvertibleCollection<TEvent> events,
        HashSet<TEvent>? poisonStreamCollection,
        out PooledList<TEvent>? withoutPoison,
        out PooledList<PoisonEvent>? poison)
    {
        poison = null;
        withoutPoison = null;

        if (poisonStreamCollection == null)
            return;

        withoutPoison = new PooledList<TEvent>(events.Count - 1);
        poison = new PooledList<PoisonEvent>(1);
        foreach (var @event in events)
        {
            if (!poisonStreamCollection.Contains(@event))
            {
                withoutPoison.Add(@event);
                continue;
            }

            poison.Add(new PoisonEvent(@event, StreamIsPoisonReason));
        }
    }

    private readonly record struct PoisonEvent
    {
        internal PoisonEvent(TEvent @event, string reason)
        {
            Event = @event;
            Reason = reason;
        }

        public TEvent Event { get; }

        public string Reason { get; }
    }
}