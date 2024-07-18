using System.Collections;

namespace Eventso.Subscription.Observing.Batch;

internal sealed class BatchHandler<TEvent> where TEvent : IEvent
{
    private readonly IEventHandler<TEvent> _eventHandler;
    private readonly IConsumer<TEvent> _consumer;
    private readonly ILogger<BatchEventObserver<TEvent>> _logger;

    public BatchHandler(
        IEventHandler<TEvent> eventHandler,
        IConsumer<TEvent> consumer,
        ILogger<BatchEventObserver<TEvent>> logger)
    {
        _eventHandler = eventHandler;
        _consumer = consumer;
        _logger = logger;
    }

    public async Task HandleBatch(
        PooledList<Buffer<TEvent>.BufferedEvent> events,
        int toBeHandledEventCount,
        CancellationToken token)
    {
        if (events.Count == 0)
            return;

        var allEvents = new EventsCollection(events);

        try
        {
            if (events.Count == toBeHandledEventCount)
            {
                await _eventHandler.Handle(allEvents, new HandlingContext(), token);
            }
            else if (toBeHandledEventCount > 0)
            {
                using var eventsToHandle = GetEventsToHandle(events.Span, toBeHandledEventCount);

                await _eventHandler.Handle(eventsToHandle, new HandlingContext(), token);
            }

            _consumer.Acknowledge(allEvents);
        }
        catch (OperationCanceledException ex)
        {
            _logger.LogError(ex, "Handling batch cancelled.");

            _consumer.Cancel();
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Handling batch exception. Start splitting batch by one.");

            await SplitBatch(events, token);

            _logger.LogInformation("Handling split batch completed successfully.");
        }
    }

    private async Task SplitBatch(PooledList<Buffer<TEvent>.BufferedEvent> messages, CancellationToken token)
    {
        foreach (var message in messages)
        {
            if (message.Skipped)
                continue;

            await _eventHandler.Handle(message.Event, new HandlingContext(IsBatchSplitPart: true), token);

            _consumer.Acknowledge(message.Event);
        }
    }

    private static PooledList<TEvent> GetEventsToHandle(
        ReadOnlySpan<Buffer<TEvent>.BufferedEvent> messages,
        int handleMessageCount)
    {
        var list = new PooledList<TEvent>(handleMessageCount);

        foreach (ref readonly var message in messages)
        {
            if (!message.Skipped)
                list.Add(message.Event);
        }

        return list;
    }

    private sealed class EventsCollection : IConvertibleCollection<TEvent>
    {
        private readonly PooledList<Buffer<TEvent>.BufferedEvent> _messages;

        public EventsCollection(PooledList<Buffer<TEvent>.BufferedEvent> messages)
            => _messages = messages;

        public int Count => _messages.Count;

        public TEvent this[int index]
            => _messages[index].Event;

        public IReadOnlyCollection<TOut> Convert<TOut>(Converter<TEvent, TOut> converter)
            => _messages.Convert(i => converter(i.Event));

        public bool OnlyContainsSame<TValue>(Func<TEvent, TValue> valueConverter)
        {
            if (Count == 0)
                return true;

            var comparer = EqualityComparer<TValue>.Default;
            var sample = valueConverter(_messages[0].Event);

            foreach (ref readonly var item in _messages.Span)
            {
                if (!comparer.Equals(valueConverter(item.Event), sample))
                    return false;
            }

            return true;
        }

        public IEnumerator<TEvent> GetEnumerator()
        {
            foreach (var item in _messages.Segment)
                yield return item.Event;
        }

        IEnumerator IEnumerable.GetEnumerator()
            => GetEnumerator();
    }
}