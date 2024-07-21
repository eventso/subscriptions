using System;
using System.Collections;

namespace Eventso.Subscription.Observing.Batch;

internal sealed class BatchHandler<TEvent>(
    IEventHandler<TEvent> eventHandler,
    IConsumer<TEvent> consumer,
    ILogger<BatchEventObserver<TEvent>> logger) where TEvent : IEvent
{
    public async Task HandleBatch(PooledList<Buffer<TEvent>.BufferedEvent> events, int toBeHandledEventCount, CancellationToken token)
    {
        if (events.Count == 0)
            return;

        var allEvents = new EventsCollection(events);

        try
        {
            if (events.Count == toBeHandledEventCount)
            {
                await eventHandler.Handle(allEvents, new HandlingContext(), token);
            }
            else if (toBeHandledEventCount > 0)
            {
                using var eventsToHandle = GetEventsToHandle(events.Span, toBeHandledEventCount);

                await eventHandler.Handle(eventsToHandle, new HandlingContext(), token);
            }

            consumer.Acknowledge(allEvents);
        }
        catch (OperationCanceledException ex)
        {
            logger.LogError(ex, "Handling batch cancelled.");

            consumer.Cancel();
            throw;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Handling batch exception. Start slicing batch by one.");

            await SliceBatch(events, token);

            logger.LogInformation("Handling sliced batch completed successfully.");
        }
    }

    private async Task SliceBatch(PooledList<Buffer<TEvent>.BufferedEvent> messages, CancellationToken token)
    {
        foreach (var message in messages)
        {
            if (message.Skipped)
                continue;

            await eventHandler.Handle(message.Event, new HandlingContext(IsBatchSlice: true), token);

            consumer.Acknowledge(message.Event);
        }
    }

    private static PooledList<TEvent> GetEventsToHandle(ReadOnlySpan<Buffer<TEvent>.BufferedEvent> messages, int handleMessageCount)
    {
        var list = new PooledList<TEvent>(handleMessageCount);

        foreach (ref readonly var message in messages)
        {
            if (!message.Skipped)
                list.Add(message.Event);
        }

        return list;
    }

    private sealed class EventsCollection(PooledList<Buffer<TEvent>.BufferedEvent> messages)
        : IConvertibleCollection<TEvent>
    {
        public int Count => messages.Count;

        public TEvent this[int index]
            => messages[index].Event;

        public IReadOnlyCollection<TOut> Convert<TOut>(Converter<TEvent, TOut> converter)
            => messages.Convert(i => converter(i.Event));

        public bool OnlyContainsSame<TValue>(Func<TEvent, TValue> valueConverter)
        {
            if (Count == 0)
                return true;

            var comparer = EqualityComparer<TValue>.Default;
            var sample = valueConverter(messages[0].Event);

            foreach (ref readonly var item in messages.Span)
            {
                if (!comparer.Equals(valueConverter(item.Event), sample))
                    return false;
            }

            return true;
        }

        public int FindFirstIndexIn(IKeySet<TEvent> set)
        {
            if (!set.IsEmpty())
            {
                var items = messages.Span;
                for (var index = 0; index < items.Length; index++)
                {
                    if (set.Contains(items[index].Event))
                        return index;
                }
            }

            return -1;
        }

        public void CopyTo(PooledList<TEvent> target, int start, int length)
        {
            foreach (ref readonly var item in messages.Span.Slice(start, length))
                target.Add(item.Event);
        }

        public IEnumerator<TEvent> GetEnumerator()
        {
            foreach (var item in messages.Segment)
                yield return item.Event;
        }

        IEnumerator IEnumerable.GetEnumerator()
            => GetEnumerator();
    }
}