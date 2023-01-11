using Eventso.Subscription.Hosting;
using Xunit.DependencyInjection;
using Xunit.DependencyInjection.Logging;

namespace Eventso.Subscription.IntegrationTests;

public sealed class TestHostStartup
{
    private readonly ITestOutputHelperAccessor _outputHelperAccessor;

    public TestHostStartup(ITestOutputHelperAccessor outputHelperAccessor)
    {
        _outputHelperAccessor = outputHelperAccessor;
    }

    public IServiceCollection CreateServiceCollection()
    {
        var services = new ServiceCollection();

        services
            .AddLogging(builder =>
                builder.AddProvider(new XunitTestOutputLoggerProvider(_outputHelperAccessor)))
            .AddSingleton(new CollectingHandler.Options(TimeSpan.FromMilliseconds(30)))
            .Scan(x => x.AddTypes(typeof(CollectingHandler)).AsSelfWithInterfaces().WithSingletonLifetime());

        return services;
    }
}