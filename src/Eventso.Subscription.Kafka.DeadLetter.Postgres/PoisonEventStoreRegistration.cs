using Eventso.Subscription.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Eventso.Subscription.Kafka.DeadLetter.Postgres;

public static class PoisonEventStoreRegistration
{
    public static DeadLetterQueueOptions UsePostgresDeadLetterQueueStore(
        this IServiceCollection services,
        DeadLetterQueueOptions options,
        Func<IServiceProvider, IConnectionFactory> connectionFactoryProvider)
    {
        services.Replace(
            ServiceDescriptor.Singleton<IPoisonEventStore>(p =>
                PoisonEventStore.Initialize(
                        connectionFactoryProvider(p),
                        options.MaxRetryAttemptCount,
                        options.MinHandlingRetryInterval,
                        options.MaxRetryDuration,
                        CancellationToken.None)
                    .GetAwaiter()
                    .GetResult()));
        return options;
    }
}