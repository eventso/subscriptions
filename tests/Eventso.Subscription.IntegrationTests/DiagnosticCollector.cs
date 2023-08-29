using System.Diagnostics;

namespace Eventso.Subscription.IntegrationTests;

internal class DiagnosticCollector : IDisposable
{
    private readonly ActivityListener _activityListener;
    private readonly List<Exception> _handlerExceptions = new();
    private readonly List<Exception> _consumingExceptions = new();
    private readonly List<Activity> _started = new();
    private readonly List<Activity> _stopped = new();


    public DiagnosticCollector()
    {
        _activityListener = new ActivityListener
        {
            ShouldListenTo = a => a.Name == Diagnostic.SourceName,
            Sample =
                (ref ActivityCreationOptions<ActivityContext> options) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStarted = a => _started.Add(a),
            ActivityStopped = a =>
            {
                _stopped.Add(a);
                if (a.GetCustomProperty("exception") is Exception ex)
                {
                    if (a.OperationName == Diagnostic.PipelineHandle)
                        _handlerExceptions.Add(ex);

                    if (a.OperationName == Diagnostic.HostConsuming)
                        _consumingExceptions.Add(ex);
                }
            }
        };

        ActivitySource.AddActivityListener(_activityListener);
    }

    public IReadOnlyCollection<Exception> HandlerExceptions
        => _handlerExceptions;

    public IReadOnlyCollection<Exception> ConsumingExceptions
        => _consumingExceptions;

    public IEnumerable<Activity> GetStarted(string name) 
        => _started.Where(activity => activity.OperationName.Equals(name));

    public IEnumerable<Activity> GetStopped(string name)
        => _stopped.Where(activity => activity.OperationName.Equals(name));
    
    public void Dispose()
    {
        _activityListener.Dispose();
    }
}