using System.Diagnostics;

namespace Eventso.Subscription.Hosting;

internal class ObservabilityEventHandler<TEvent> : IEventHandler<TEvent>
    where TEvent : IEvent
{
    private readonly IEventHandler<TEvent> _inner;
    private readonly ILogger _logger;
    private readonly IGroupedMetadataProvider<TEvent> _groupedMetadataProvider;

    public ObservabilityEventHandler(IEventHandler<TEvent> inner, ILogger logger, IGroupedMetadataProvider<TEvent> groupedMetadataProvider)
    {
        _inner = inner;
        _logger = logger;
        _groupedMetadataProvider = groupedMetadataProvider;
    }

    public async Task Handle(TEvent @event, CancellationToken cancellationToken)
    {
        var metadata = @event.GetMetadata();

        using var loggingScope = CreateSingleLoggingScope(metadata);
        using var tracingScope = CreateSingleActivity(metadata);

        try
        {
            await _inner.Handle(@event, cancellationToken);
        }
        catch (Exception ex)
        {
            tracingScope.Activity?.SetException(ex);
            throw;
        }
    }

    private IDisposable? CreateSingleLoggingScope(IReadOnlyCollection<KeyValuePair<string, object>> metadata)
    {
        return metadata.Count > 0 ? _logger.BeginScope(metadata) : null;
    }

    private static Diagnostic.RootActivityScope CreateSingleActivity(IReadOnlyCollection<KeyValuePair<string, object>> metadata)
    {
        var scope = Diagnostic.StartRooted("event.handle");
        if (metadata.Count > 0 && scope.Activity is { } activity)
        {
            foreach (var (tagKey, tagValue) in metadata)
            {
                activity.AddTag(tagKey, tagValue);
            }
        }

        return scope;
    }

    public async Task Handle(IConvertibleCollection<TEvent> events, CancellationToken cancellationToken)
    {
        var groupedMetadata = _groupedMetadataProvider.GetFor(events);

        using var loggingScope = CreateBatchLoggingScope(groupedMetadata);
        using var tracingScope = CreateBatchActivity(groupedMetadata);

        try
        {
            await _inner.Handle(events, cancellationToken);
        }
        catch (Exception ex)
        {
            tracingScope.Activity?.SetException(ex);
            throw;
        }
    }

    private IDisposable? CreateBatchLoggingScope(List<KeyValuePair<string, object>[]> groupedMetadata)
    {
        KeyValuePair<string, object>[] loggingScopeState = [KeyValuePair.Create("@eventso_batch", (object)groupedMetadata)];

        return _logger.BeginScope(loggingScopeState);
    }

    private static Diagnostic.RootActivityScope CreateBatchActivity(List<KeyValuePair<string, object>[]> groupedMetadata)
    {
        var scope = Diagnostic.StartRooted("batch.handle");
        if (scope.Activity is { } activity)
        {
            var ts = DateTimeOffset.UtcNow;
            foreach (var metadataGroup in groupedMetadata)
            {
                activity.AddEvent(new ActivityEvent(
                    name: "batch.metadata",
                    timestamp: ts,
                    tags: new ActivityTagsCollection(metadataGroup!)));
            }
        }

        return scope;
    }
}
