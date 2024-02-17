using System.Diagnostics;
using Microsoft.Extensions.Logging.Abstractions;
using OpenTelemetry.Trace;

namespace Eventso.Subscription;

public static class Diagnostic
{
    public static readonly string HostConsuming = "host.consuming";
    public static readonly string PipelineHandle = "pipeline.handle";
    public static readonly string EventHandlerHandle = "eventhandler.handle";
    public static readonly string SourceName = "eventso";

    public static readonly ActivitySource ActivitySource = new(SourceName);

    public static Activity SetException(this Activity activity, Exception ex)
    {
        activity.SetStatus(ActivityStatusCode.Error, ex.Message)
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
}

public sealed class TimeoutLogger : IDisposable
{
    private readonly string _name;
    private readonly int _minutes;
    private static ILogger _logger = NullLogger.Instance;

    public static void Init(ILogger logger)
    {
        _logger = logger;
    }

    private readonly Timer _timer;

    public TimeoutLogger(string callName, int minutes)
    {
        _name = callName;
        _minutes = minutes;
        _timer = new Timer(Log, null, TimeSpan.FromMinutes(minutes), Timeout.InfiniteTimeSpan);
    }

    private void Log(object? _)
    {
        _logger.LogError($"{_name} call takes more than {_minutes} minutes.");
    }

    public void Dispose()
    {
        _timer.Dispose();
    }
}