using Eventso.Subscription.Configurations;
using Eventso.Subscription.Hosting;
using Eventso.Subscription.Pipeline;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

namespace Eventso.Subscription.Kafka.DeadLetter;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddDeadLetterQueue(
        this IServiceCollection services,
        Func<IServiceProvider, DeadLetterQueueOptions> provideOptions,
        IDeadLetterStoreRegistration storeRegistration)
    {
        services.RemoveAll<IPoisonEventQueueFactory>();
        services.RemoveAll<DeadLetterQueueOptions>();
        services.RemoveAll<IPoisonEventStore>();
        services.RemoveAll<IPoisonEventRetryScheduler>();
        services.RemoveAll<IPoisonEventQueueRetryingService>();
        
        services.TryAddSingleton(provideOptions);
        
        services.TryAddSingleton<IPoisonEventQueueFactory, PoisonEventQueueFactory>();
        services.TryAddSingleton<IPoisonEventQueueRetryingService, PoisonEventQueueRetryingService>();
        services.AddHostedService<PoisonEventQueueRetryingHost>();
        services.AddHostedService<PoisonEventQueueMetricCollector>();

        storeRegistration.Register(services);

        //overrides
        services.RemoveAll<IConsumerFactory>();
        services.TryAddSingleton<IConsumerFactory, DeadLetter.DeadLetterKafkaConsumerFactory>();

        services.RemoveAll<IMessagePipelineFactory>();
        services.TryAddSingleton<IMessagePipelineFactory>(p =>
            new MessagePipelineFactory(
                p.GetRequiredService<IMessageHandlerScopeFactory>(),
                DefaultRetryingStrategy.GetDefaultShortRetryBuilder(p.GetRequiredService<ILoggerFactory>().CreateLogger<RetryingAction>()).Build(),
                p.GetRequiredService<ILoggerFactory>()));

        return services;
    }

    public interface IDeadLetterStoreRegistration
    {
        void Register(IServiceCollection services);
    }
}