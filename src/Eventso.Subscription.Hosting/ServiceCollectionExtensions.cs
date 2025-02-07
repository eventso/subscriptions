using Eventso.Subscription.Hosting.DeadLetter;
using Eventso.Subscription.Kafka.DeadLetter;
using Eventso.Subscription.Pipeline;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Scrutor;

namespace Eventso.Subscription.Hosting;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddSubscriptions(
        this IServiceCollection services,
        Action<ISubscriptionCollection, IServiceProvider> subscriptions,
        Func<ITypeSourceSelector, IImplementationTypeSelector>? handlersSelector = null,
        ServiceLifetime handlersLifetime = ServiceLifetime.Scoped)
    {
        TryAddSubscriptionServices(services);

        services.AddSingleton<ISubscriptionCollection, SubscriptionCollection>(
            p =>
            {
                var collection = new SubscriptionCollection();
                subscriptions(collection, p);
                return collection;
            });

        if (handlersSelector != null)
            services.Scan(t => handlersSelector(t)
                .AddClasses(c => c.AssignableTo(typeof(IMessageHandler<>)))
                .UsingRegistrationStrategy(new IgnoreDuplicateRegistrationStrategy())
                .AsImplementedInterfaces()
                .WithLifetime(handlersLifetime));

        // just for DI work
        services.TryAddSingleton<IPoisonEventQueueFactory>(DisabledDeadLetterQueue.Instance);

        return services;
    }

    public static void AddDevNullDeadLetterQueue(this IServiceCollection services)
    {
        services.AddDeadLetterQueue(
            _ => { },
            _ => DevNullPoisonEventStore.Instance,
            (_, _) => DevNullPoisonEventStore.Instance);
    }

    // TODO make more like MvcBuilder or smth
    public static void AddDeadLetterQueue(
        this IServiceCollection services,
        Action<DeadLetterQueueOptions> configureOptions,
        Func<IServiceProvider, IPoisonEventStore> provideStore,
        Func<IServiceProvider, DeadLetterQueueOptions.RetrySchedulingOptions, IPoisonEventRetryScheduler> provideRetryingScheduler)
    {
        services.RemoveAll<IPoisonEventQueueFactory>();
        services.RemoveAll<DeadLetterQueueOptions>();
        services.RemoveAll<IPoisonEventStore>();
        services.RemoveAll<IPoisonEventRetryScheduler>();
        services.RemoveAll<IPoisonEventQueueRetryingService>();

        var options = new DeadLetterQueueOptions();
        configureOptions(options);
        services.TryAddSingleton(options);

        services.TryAddSingleton(provideStore);
        services.TryAddSingleton<IPoisonEventRetryScheduler>(sp =>
            provideRetryingScheduler(
                sp,
                sp.GetRequiredService<DeadLetterQueueOptions>().GetRetrySchedulingOptions()));
        services.TryAddSingleton<IPoisonEventQueueFactory, PoisonEventQueueFactory>();
        services.TryAddSingleton<IPoisonEventQueueRetryingService, PoisonEventQueueRetryingService>();
        services.AddHostedService<PoisonEventQueueRetryingHost>();
        services.AddHostedService<PoisonEventQueueMetricCollector>();
    }

    private static void TryAddSubscriptionServices(IServiceCollection services)
    {
        services.TryAddSingleton<SubscriptionHost>();
        services.AddHostedService<SubscriptionHost>(p => p.GetRequiredService<SubscriptionHost>());
        services.TryAddSingleton<ISubscriptionHost>(p => p.GetRequiredService<SubscriptionHost>());
        services.TryAddSingleton<IMessageHandlerScopeFactory, MessageHandlerScopeFactory>();
        services.TryAddSingleton<IMessagePipelineFactory>(p =>
            new MessagePipelineFactory(
                p.GetRequiredService<IMessageHandlerScopeFactory>(),
                p.GetRequiredService<ILoggerFactory>()));
        services.TryAddSingleton<IMessageHandlersRegistry>(s => MessageHandlersRegistry.Create(services));
        services.TryAddSingleton<IConsumerFactory, KafkaConsumerFactory>();
    }

    private sealed class IgnoreDuplicateRegistrationStrategy : RegistrationStrategy
    {
        public override void Apply(IServiceCollection services, ServiceDescriptor descriptor)
        {
            foreach (var d in services)
            {
                if (d.ServiceType == descriptor.ServiceType &&
                    GetImplementationType(d) == GetImplementationType(descriptor))
                    return;
            }

            services.Add(descriptor);
        }

        private static Type? GetImplementationType(ServiceDescriptor descriptor)
        {
            if (descriptor.ImplementationType != null)
                return descriptor.ImplementationType;

            if (descriptor.ImplementationInstance != null)
                return descriptor.ImplementationInstance.GetType();

            if (descriptor.ImplementationFactory != null)
            {
                var typeArguments = descriptor.ImplementationFactory
                    .GetType().GenericTypeArguments;

                return typeArguments[1];
            }

            return null;
        }
    }
}