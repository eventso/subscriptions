using Xunit.DependencyInjection.Logging;

[assembly: CollectionBehavior(CollectionBehavior.CollectionPerAssembly)]

namespace Eventso.Subscription.IntegrationTests;

public class Startup
{
    public void ConfigureServices(IServiceCollection services)
    {
        services.AddSingleton<KafkaConfig>()
            .AddSingleton<KafkaJsonProducer>();

        services.AddLogging(lb => lb.AddXunitOutput());

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
}