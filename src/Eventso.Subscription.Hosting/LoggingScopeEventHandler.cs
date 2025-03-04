namespace Eventso.Subscription.Hosting;

public sealed class LoggingScopeEventHandler<TEvent> : IEventHandler<TEvent> where TEvent : IEvent
{
    private readonly IEventHandler<TEvent> _inner;
    private readonly ILogger _logger;
    private readonly KeyValuePair<string, object>[] _metadata;

    public LoggingScopeEventHandler(IEventHandler<TEvent> inner, string topic, ILogger logger)
    {
        _inner = inner;
        _logger = logger;
        _metadata = [new KeyValuePair<string, object>("eventso_topic", topic)];
    }

    public async Task Handle(TEvent @event, HandlingContext context, CancellationToken token)
    {
        var metadata = @event.GetMetadata();

        using var scope = metadata.Count > 0 ? _logger.BeginScope(metadata) : null;

        await _inner.Handle(@event, context, token);
    }

    public async Task Handle(IConvertibleCollection<TEvent> events, HandlingContext context, CancellationToken token)
    {
        using var scope = _logger.BeginScope(_metadata);

        await _inner.Handle(events, context, token);
    }
}