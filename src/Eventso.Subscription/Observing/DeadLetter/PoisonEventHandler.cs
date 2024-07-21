namespace Eventso.Subscription.Observing.DeadLetter;

public sealed class PoisonEventHandler<TEvent>(
    string topic,
    IPoisonEventInbox<TEvent> poisonEventInbox,
    IEventHandler<TEvent> inner) : IEventHandler<TEvent> where TEvent : IEvent
{
    internal const string StreamIsPoisonReason = "Event stream is poisoned.";

    public async Task Handle(TEvent @event, HandlingContext context, CancellationToken token)
    {
        var poisonEventKeys = await poisonEventInbox.GetEventKeys(topic);

        if (poisonEventKeys.Contains(@event))
        {
            await poisonEventInbox.Add(@event, StreamIsPoisonReason, token);
            return;
        }

        try
        {
            await inner.Handle(@event, context, token);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            await poisonEventInbox.Add(@event, exception.ToString(), token);
        }
    }

    public async Task Handle(IConvertibleCollection<TEvent> events, HandlingContext context, CancellationToken token)
    {
        var poisonEventKeys = await poisonEventInbox.GetEventKeys(topic);

        var firstPoisonEventIndex = events.FindFirstIndexIn(poisonEventKeys);

        if (firstPoisonEventIndex == -1)
        {
            await inner.Handle(events, context, token);
            return;
        }

        await poisonEventInbox.Add(events[firstPoisonEventIndex], StreamIsPoisonReason, token);

        var clearedEvents = new PooledList<TEvent>(events.Count - 1);
        events.CopyTo(clearedEvents, start: 0, length: firstPoisonEventIndex);

        for (var index = firstPoisonEventIndex + 1; index < events.Count; index++)
        {
            var @event = events[index];

            if (poisonEventKeys.Contains(@event))
                await poisonEventInbox.Add(@event, StreamIsPoisonReason, token);
            else
                clearedEvents.Add(@event);
        }

        await inner.Handle(clearedEvents, context, token);
    }
}