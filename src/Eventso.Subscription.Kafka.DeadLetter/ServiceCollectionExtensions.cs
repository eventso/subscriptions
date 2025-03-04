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
        Func<IServiceProvider, IPoisonEventStore> provideStore,
        Func<IServiceProvider, DeadLetterQueueOptions.RetrySchedulingOptions, IPoisonEventRetryScheduler> provideRetryingScheduler)
    {
        services.RemoveAll<IPoisonEventQueueFactory>();
        services.RemoveAll<DeadLetterQueueOptions>();
        services.RemoveAll<IPoisonEventStore>();
        services.RemoveAll<IPoisonEventRetryScheduler>();
        services.RemoveAll<IPoisonEventQueueRetryingService>();
        
        services.TryAddSingleton(provideOptions);

        services.TryAddSingleton(provideStore);
        services.TryAddSingleton<IPoisonEventRetryScheduler>(sp =>
            provideRetryingScheduler(
                sp,
                sp.GetRequiredService<DeadLetterQueueOptions>().GetRetrySchedulingOptions()));
        services.TryAddSingleton<IPoisonEventQueueFactory, PoisonEventQueueFactory>();
        services.TryAddSingleton<IPoisonEventQueueRetryingService, PoisonEventQueueRetryingService>();
        services.AddHostedService<PoisonEventQueueRetryingHost>();
        services.AddHostedService<PoisonEventQueueMetricCollector>();


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
}