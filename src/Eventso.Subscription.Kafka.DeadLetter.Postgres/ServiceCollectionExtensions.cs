using Eventso.Subscription.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Eventso.Subscription.Kafka.DeadLetter.Postgres;

public static class ServiceCollectionExtensions
{
    // make public when ready
    public static void AddPostgresDeadLetterQueue<TConnectionFactory>(
        this IServiceCollection services,
        Action<DeadLetterQueueOptions>? configureOptions = default,
        bool installSchema = false)
        where TConnectionFactory : class, IConnectionFactory
    {
        services.RemoveAll<IConnectionFactory>();
        services.RemoveAll<PoisonEventSchemaInitializer>();

        services.AddSingleton<IConnectionFactory, TConnectionFactory>();
        services.AddSingleton<PoisonEventSchemaInitializer>(sp =>
        {
            if (installSchema)
            {
                PoisonEventSchemaInitializer.Initialize(
                        sp.GetRequiredService<IConnectionFactory>(),
                        CancellationToken.None)
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
                return new PoisonEventStore(provider.GetRequiredService<IConnectionFactory>());
            },
            (provider, options) =>
            {
                _ = provider.GetRequiredService<PoisonEventSchemaInitializer>();
                return new PoisonEventRetryingScheduler(
                    provider.GetRequiredService<IConnectionFactory>(),
                    options.MaxRetryAttemptCount,
                    options.MinHandlingRetryInterval,
                    options.MaxRetryDuration);
            });
    }
}