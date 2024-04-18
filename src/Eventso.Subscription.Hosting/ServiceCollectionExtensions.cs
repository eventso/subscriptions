using Eventso.Subscription.Kafka.DeadLetter;
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
        Action<IServiceCollection, DeadLetterQueueOptions>? configureDeadLetterQueue)
    {
        DeadLetterQueueOptions dlqOptions;

        if (configureDeadLetterQueue != null)
        {
            dlqOptions = new DeadLetterQueueOptions();
            configureDeadLetterQueue(services, dlqOptions);
        }
        else
        {
            dlqOptions = DeadLetterQueueOptions.Disabled;
        }

        services.TryAddSingleton<IPoisonEventStore>(_ => throw new Exception("DLQ store was not configured"));
        services.TryAddSingleton<IPoisonEventQueueFactory, PoisonEventQueueFactory>();
        services.TryAddSingleton<IDeadLetterQueueScopeFactory>(AsyncLocalDeadLetterWatcher.Instance);
        services.TryAddSingleton(dlqOptions);
        services.AddHostedService<PoisonEventRetryingHost>();
        services.TryAddSingleton<IDeadLetterQueue>(
            sp => sp.GetRequiredService<DeadLetterQueueOptions>().IsEnabled
                ? AsyncLocalDeadLetterWatcher.Instance
                // disable api for clients
                : throw new Exception("DLQ was not configured."));
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