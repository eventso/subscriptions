namespace Eventso.Subscription.Observing.DeadLetter;

public sealed class PoisonEventHandler<TEvent>(
    IPoisonEventInbox<TEvent> poisonEventInbox,
    IEventHandler<TEvent> inner,
    ILogger<PoisonEventHandler<TEvent>> logger)
    : IEventHandler<TEvent>
    where TEvent : IEvent
{
    internal const string StreamIsPoisonReason = "Event stream is poisoned.";

    public async Task Handle(TEvent @event, CancellationToken cancellationToken)
    {
        if (await poisonEventInbox.IsPartOfPoisonStream(@event, cancellationToken))
        {
            await poisonEventInbox.Add(@event, StreamIsPoisonReason, cancellationToken);
            return;
        }

        PoisonEvent<TEvent>? poisonEvent = null;
        try
        {
            await inner.Handle(@event, cancellationToken);
        }
        catch (Exception exception)
        {
            poisonEvent = new PoisonEvent<TEvent>(@event, exception.ToString());
        }

        if (poisonEvent == null)
            return;

        await poisonEventInbox.Add(poisonEvent.Value.Event, poisonEvent.Value.Reason, cancellationToken);
    }

    public async Task Handle(IConvertibleCollection<TEvent> events, CancellationToken token)
    {
        var poisonStreamCollection = await GetPoisonStreams(events, token);

        FilterPoisonEvents(events, poisonStreamCollection, out var withoutPoison, out var poison);

        var healthyEvents = withoutPoison ?? events;

        try
        {
            await inner.Handle(healthyEvents, token);
        }
        catch (Exception exception) when (healthyEvents.Count == 1)
        {
            poison ??= new PooledList<PoisonEvent<TEvent>>(1);
            poison.Add(new PoisonEvent<TEvent>(healthyEvents[0], exception.ToString()));
        }
        catch (Exception)
        {
            // todo this unwrapping should be executed in some other layer
            // unwrapping collection
            logger.LogInformation(
                "[{ParentScope}][{Scope}] Unwrapping {HealthyEventsCount} healthy events to find poison",
                nameof(Eventso),
                nameof(PoisonEventHandler<TEvent>),
                healthyEvents.Count);

            for (var i = 1; i <= healthyEvents.Count; i++)
            {
                using var pooledList = new PooledList<TEvent>(1); // expensive, but...
                pooledList.Add(healthyEvents[i - 1]);
                await Handle(pooledList, token); // will not be really recursive because of catch block above

                if (i % 200 == 0)
                    logger.LogInformation(
                        "[{ParentScope}][{Scope}] Unwrapped {UnwrappedCount} of {HealthyEventsCount} healthy events to find poison",
                        nameof(Eventso),
                        nameof(PoisonEventHandler<TEvent>),
                        i,
                        healthyEvents.Count);
            }

            logger.LogInformation(
                "[{ParentScope}][{Scope}] Unwrapped {HealthyEventsCount} healthy events to find poison",
                nameof(Eventso),
                nameof(PoisonEventHandler<TEvent>),
                healthyEvents.Count);
        }

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
        out PooledList<PoisonEvent<TEvent>>? poison)
    {
        poison = null;
        withoutPoison = null;

        if (poisonStreamCollection == null)
            return;

        withoutPoison = new PooledList<TEvent>(events.Count - 1);
        poison = new PooledList<PoisonEvent<TEvent>>(1);
        foreach (var @event in events)
        {
            if (!poisonStreamCollection.Contains(@event))
            {
                withoutPoison.Add(@event);
                continue;
            }

            poison.Add(new PoisonEvent<TEvent>(@event, StreamIsPoisonReason));
        }
    }
}