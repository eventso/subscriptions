namespace Eventso.Subscription.Observing.Batch;

public sealed class SingleTypeLastByKeyEventHandler<TEvent> : IEventHandler<TEvent>
    where TEvent : IEvent
{
    private readonly IEventHandler<TEvent> _nextHandler;

    public SingleTypeLastByKeyEventHandler(IEventHandler<TEvent> nextHandler)
        => _nextHandler = nextHandler;

    public Task Handle(TEvent @event, HandlingContext context, CancellationToken cancellationToken)
        => _nextHandler.Handle(@event, context, cancellationToken);

    public async Task Handle(IConvertibleCollection<TEvent> events, HandlingContext context, CancellationToken token)
    {
        if (events.Count == 0)
            return;

        var dictionary = new Dictionary<Guid, TEvent>(events.Count);

        for (var i = 0; i < events.Count; ++i)
        {
            var @event = events[i];
            dictionary[@event.GetKey()] = @event;
        }

        using var lastEvents = new PooledList<TEvent>(dictionary.Count);
        foreach (var (_, @event) in dictionary)
            lastEvents.Add(@event);

        await _nextHandler.Handle(lastEvents, context, token);
    }
}
