using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;

namespace Eventso.Subscription;

public static class Diagnostic
{
    public static readonly string HostConsuming = "host.consuming";
    public static readonly string PipelineHandle = "pipeline.handle";
    public static readonly string EventHandlerHandle = "eventhandler.handle";
    public static readonly string SourceName = "eventso";
    public static readonly string MeterName = "eventso.subscription";

    public static readonly ActivitySource ActivitySource = new(SourceName);
    public static readonly Meter Meter = new(MeterName);

    public static Activity SetException(this Activity activity, Exception ex)
    {
        activity
            .SetStatus(ActivityStatusCode.Error, ex.Message)
            .SetCustomProperty("exception", ex);

        activity.RecordException(ex);

        return activity;
    }

    internal static RootActivityScope StartRooted(string name, ActivityKind kind = ActivityKind.Internal)
    {
        var previous = Activity.Current;
        Activity.Current = null;

        var newRoot = ActivitySource.StartActivity(name, kind);

        return new RootActivityScope(newRoot, previous);
    }

    internal readonly struct RootActivityScope : IDisposable
    {
        public readonly Activity? Activity;

        private readonly Activity? _previous;

        public RootActivityScope(Activity? newRoot, Activity? previous)
        {
            Activity = newRoot;
            _previous = previous;
        }

        public void Dispose()
        {
            Activity?.Dispose();

            if (_previous is { } previous)
            {
                Activity.Current = previous;
            }
        }
    }

    //from OpenTelemetry.Api
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void RecordException(this Activity activity, Exception ex)
    {
        const string AttributeExceptionEventName = "exception";
        const string AttributeExceptionType = "exception.type";
        const string AttributeExceptionMessage = "exception.message";
        const string AttributeExceptionStacktrace = "exception.stacktrace";

        if (ex == null || activity == null)
        {
            return;
        }

        var tagsCollection = new ActivityTagsCollection
        {
            { AttributeExceptionType, ex.GetType().FullName },
            { AttributeExceptionStacktrace, ex.ToString() },
        };

        if (!string.IsNullOrWhiteSpace(ex.Message))
        {
            tagsCollection.Add(AttributeExceptionMessage, ex.Message);
        }

        activity.AddEvent(new ActivityEvent(AttributeExceptionEventName, default, tagsCollection));
    }
}