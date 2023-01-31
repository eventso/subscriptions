using System.Diagnostics;

namespace Eventso.Subscription.IntegrationTests;

internal class DiagnosticExceptionCollector : IDisposable
{
    private readonly ActivityListener _activityListener;
    private readonly List<Exception> _handlerExceptions = new();
    private readonly List<Exception> _consumingExceptions = new();


    public DiagnosticExceptionCollector()
    {
        _activityListener = new ActivityListener
        {
            ShouldListenTo = a => a.Name == Diagnostic.SourceName,
            Sample =
                (ref ActivityCreationOptions<ActivityContext> options) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = a =>
            {
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

    public void Dispose()
    {
        _activityListener.Dispose();
    }
}