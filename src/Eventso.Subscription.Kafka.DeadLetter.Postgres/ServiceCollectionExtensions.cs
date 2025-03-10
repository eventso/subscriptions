using Eventso.Subscription.Hosting;
using Eventso.Subscription.Hosting.DeadLetter;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Eventso.Subscription.Kafka.DeadLetter.Postgres;

public static class ServiceCollectionExtensions
{
    public static void AddPostgresDeadLetterQueue(
        this IServiceCollection services,
        Func<IServiceProvider, IConnectionFactory> connectionFactoryProvider,
        Action<DeadLetterQueueOptions>? configureOptions = default,
        bool installSchema = false)
    {
        services.RemoveAll<PoisonEventSchemaInitializer>();

        services.AddSingleton<PoisonEventSchemaInitializer>(sp =>
        {
            if (installSchema)
            {
                var connectionFactory = connectionFactoryProvider(sp);
                PoisonEventSchemaInitializer.Initialize(connectionFactory, CancellationToken.None)
                    .GetAwaiter()
                    .GetResult();
            }

            return PoisonEventSchemaInitializer.Completed;
        });

        services.AddDeadLetterQueue(
            configureOptions ?? (_ => { }),
            provider =>
            {
                _ = provider.GetRequiredService<PoisonEventSchemaInitializer>();
                return new PoisonEventStore(connectionFactoryProvider(provider));
            },
            (provider, options) =>
            {
                _ = provider.GetRequiredService<PoisonEventSchemaInitializer>();
                return new PoisonEventRetryScheduler(
                    connectionFactoryProvider(provider),
                    options.MaxRetryAttemptCount,
                    options.MinHandlingRetryInterval,
                    options.MaxRetryDuration);
            });
    }
}