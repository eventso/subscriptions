using Eventso.Subscription.Kafka.DeadLetter.Store;
using Eventso.Subscription.Observing.DeadLetter;
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
        ServiceLifetime handlersLifetime = ServiceLifetime.Scoped,
        Action<IServiceCollection, DeadLetterQueueOptions>? configureDeadLetterQueue = default)
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

        AddDeadLetterQueueServices(services, configureDeadLetterQueue);

        return services;
    }

    private static void AddDeadLetterQueueServices(
        IServiceCollection services,
        Action<IServiceCollection,DeadLetterQueueOptions>? configureDeadLetterQueue)
    {
        if (configureDeadLetterQueue == null)
        {
            services.AddSingleton<IPoisonEventStore>(NoDeadLetterQueue.Instance);
            services.AddSingleton<IDeadLetterQueue>(NoDeadLetterQueue.Instance);
            services.AddSingleton<IDeadLetterQueueScopeFactory>(NoDeadLetterQueue.Instance);

            // just for DI, no real influence without other services
            services.AddSingleton(new DeadLetterQueueOptions());

            return;
        }

        services.AddHostedService<PoisonEventRetryingHost>();
        services.AddSingleton<IDeadLetterQueueScopeFactory>(AsyncLocalDeadLetterWatcher.Instance);
        services.AddSingleton<IDeadLetterQueue>(AsyncLocalDeadLetterWatcher.Instance);

        var dlqOptions = new DeadLetterQueueOptions();

        configureDeadLetterQueue(services, dlqOptions);
        services.AddSingleton(dlqOptions);
    }

    private static void TryAddSubscriptionServices(IServiceCollection services)
    {
        services.TryAddSingleton<SubscriptionHost>();
        services.AddHostedService<SubscriptionHost>(p => p.GetRequiredService<SubscriptionHost>());
        services.TryAddSingleton<ISubscriptionHost>(p => p.GetRequiredService<SubscriptionHost>());
        services.TryAddSingleton<IMessageHandlerScopeFactory, MessageHandlerScopeFactory>();
        services.TryAddSingleton<IMessagePipelineFactory, MessagePipelineFactory>();
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