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

    public IServiceCollection CreateServiceCollection(TimeSpan? delay = default)
    {
        var services = new ServiceCollection();

        services
            .AddSingleton<ITestOutputHelperAccessor>(_outputHelperAccessor)
            .AddLogging(builder =>
                builder.AddXunitOutput())
            .AddSingleton(new CollectingHandler.Options(delay ?? TimeSpan.FromMilliseconds(1)))
            .Scan(x => x.FromTypes(typeof(CollectingHandler))
                .AsSelfWithInterfaces()
                .WithSingletonLifetime());

        return services;
    }
}