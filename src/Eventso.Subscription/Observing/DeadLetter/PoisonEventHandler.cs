namespace Eventso.Subscription.Observing.DeadLetter;

public sealed class PoisonEventHandler<TEvent>(
    IPoisonEventInbox<TEvent> poisonEventInbox,
    IDeadLetterQueueScopeFactory deadLetterQueueScopeFactory,
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

        using var dlqScope = deadLetterQueueScopeFactory.Create(@event);

        PoisonEvent<TEvent>? poisonEvent = null;
        try
        {
            await inner.Handle(@event, cancellationToken);
        }
        catch (Exception exception)
        {
            poisonEvent = new PoisonEvent<TEvent>(@event, exception.ToString());
        }

        poisonEvent = Coalesce(poisonEvent, dlqScope);
        if (poisonEvent == null)
            return;

        await poisonEventInbox.Add(poisonEvent.Value.Event, poisonEvent.Value.Reason, cancellationToken);

        static PoisonEvent<TEvent>? Coalesce(PoisonEvent<TEvent>? @event, IDeadLetterQueueScope<TEvent> dlqScope)
        {
            if (@event != null)
                return @event;

            var dlqPoisonEvents = dlqScope.GetPoisonEvents();
            return dlqPoisonEvents.Count != 0 ? dlqPoisonEvents.Single() : null;
        }
    }

    public async Task Handle(IConvertibleCollection<TEvent> events, CancellationToken token)
    {
        var poisonStreamCollection = await GetPoisonStreams(events, token);

        FilterPoisonEvents(events, poisonStreamCollection, out var withoutPoison, out var poison);

        var healthyEvents = withoutPoison ?? events;
        using var dlqScope = deadLetterQueueScopeFactory.Create(healthyEvents);

        try
        {
            await inner.Handle(healthyEvents, token);

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
        catch (Exception)
        {
            // todo this unwrapping should be executed in some other layer
            // unwrapping collection
            logger.LogInformation("[{ParentScope}][{Scope}] Unwrapping {HealthyEventsCount} healthy events to find poison one",
                nameof(Eventso),
                nameof(PoisonEventHandler<TEvent>),
                healthyEvents.Count);
            foreach (var healthyEvent in healthyEvents)
            {
                using var pooledList = new PooledList<TEvent>(1); // expensive, but...
                pooledList.Add(healthyEvent);
                await Handle(pooledList, token); // will not be really recursive because of catch block above
            }
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