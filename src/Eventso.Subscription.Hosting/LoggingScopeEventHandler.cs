namespace Eventso.Subscription.Hosting;

public sealed class LoggingScopeEventHandler<TEvent> : IEventHandler<TEvent> where TEvent : IEvent
{
    private readonly IEventHandler<TEvent> _inner;
    private readonly ILogger _logger;
    private readonly IEnumerable<KeyValuePair<string, string>> _metadata;

    public LoggingScopeEventHandler(IEventHandler<TEvent> inner, string topic, ILogger logger)
    {
        _inner = inner;
        _logger = logger;
        _metadata = Enumerable.Repeat(KeyValuePair.Create("eventso_topic", topic), 1);
    }

    public async Task Handle(TEvent @event, CancellationToken cancellationToken)
    {
        var metadata = @event.GetMetadata();

        using var scope = metadata.Count > 0 ? _logger.BeginScope(metadata) : null;

        await _inner.Handle(@event, cancellationToken);
    }

    public async Task Handle(IConvertibleCollection<TEvent> events, CancellationToken cancellationToken)
    {
        using var scope = _logger.BeginScope(_metadata);

        await _inner.Handle(events, cancellationToken);
    }
}