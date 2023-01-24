using Xunit.DependencyInjection;
using Xunit.DependencyInjection.Logging;

namespace Eventso.Subscription.IntegrationTests;

public class Startup
{
    public void ConfigureServices(IServiceCollection services)
    {
        services.AddSingleton<KafkaConfig>()
            .AddSingleton<KafkaJsonProducer>();

        services
            .AddTransient<IFixture>(_ =>
            {
                var fixture = new Fixture();
                fixture.Customize(new AutoNSubstituteCustomization() { ConfigureMembers = true });
                return fixture;
            })
            .AddTransient<TopicSource>()
            .AddTransient<TestHostStartup>();
    }

    public void Configure(ILoggerFactory loggerFactory, ITestOutputHelperAccessor accessor)
        => loggerFactory.AddProvider(new XunitTestOutputLoggerProvider(accessor));
}