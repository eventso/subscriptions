using System.Threading;
using Eventso.Subscription.Hosting;
using Eventso.Subscription.Kafka.DeadLetter.Store;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Eventso.Subscription.Kafka.DeadLetter.Postgres
{
    public static class PoisonEventStoreRegistration
    {
        public static DeadLetterQueueOptions UsePostgresStore<T>(
            this DeadLetterQueueOptions options,
            IServiceCollection services)
            where T : class, IConnectionFactory
        {
            services.TryAddSingleton<IConnectionFactory, T>();

            services.Replace(
                ServiceDescriptor.Singleton<IPoisonEventStore>(p =>
                    PoisonEventStore.Initialize(
                            p.GetRequiredService<IConnectionFactory>(),
                            options.MaxRetryAttemptCount,
                            options.MinHandlingRetryInterval,
                            options.MaxRetryDuration,
                            CancellationToken.None)
                        .GetAwaiter().GetResult()));

            return options;
        }
    }
}