using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Eventso.Subscription.Kafka.DeadLetter.Postgres;

public sealed class PostgresDeadLetterQueueRegistration(
    Func<IServiceProvider, IConnectionFactory> connectionFactoryProvider)
    : ServiceCollectionExtensions.IDeadLetterStoreRegistration
{
    public void Register(IServiceCollection services)
    {
        services.RemoveAll<IPoisonEventStore>();
        services.RemoveAll<IPoisonEventRetryScheduler>();

        services.TryAddSingleton<IConnectionFactory>(connectionFactoryProvider);
        services.TryAddSingleton<IPoisonEventStore, PoisonEventStore>();
        services.TryAddSingleton<IPoisonEventRetryScheduler, PoisonEventRetryScheduler>();
    }
}