using System.Diagnostics;
using Eventso.Subscription.Hosting;

namespace Eventso.Subscription.IntegrationTests;

public sealed class TestHost : IAsyncDisposable
{
    private readonly ServiceProvider _provider;
    private readonly SubscriptionHost _host;
    private readonly ActivityListener _activityListener;
    private readonly TaskCompletionSource _tcs = new();


    public TestHost(IServiceCollection serviceCollection)
    {
        var provider = serviceCollection.BuildServiceProvider();

        var host = provider.GetRequiredService<IEnumerable<IHostedService>>()
            .OfType<SubscriptionHost>()
            .Single();

        _provider = provider;
        _host = host;
        _activityListener = new ActivityListener
        {
            ShouldListenTo = a => a.Name == Diagnostic.SourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> options) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = a =>
            {
                var exception = a.OperationName == Diagnostic.HostConsuming
                    ? a.GetCustomProperty("exception") as Exception
                    : null;

                if (exception != null)
                    _tcs.SetException(exception);
            }
        };

        ActivitySource.AddActivityListener(_activityListener);
    }

    public Task FailedCompletion
        => _tcs.Task;

    public IServiceProvider ServiceProvider => _provider;

    public Task Start()
    {
        return _host.StartAsync(CancellationToken.None);
    }

    public CollectingHandler GetHandler()
        => _provider.GetRequiredService<CollectingHandler>();

    public async ValueTask DisposeAsync()
    {
        await _host.StopAsync(CancellationToken.None);
        _host.Dispose();
        await _provider.DisposeAsync();
        _activityListener.Dispose();
    }
}