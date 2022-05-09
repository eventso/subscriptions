using System;
using System.Linq;
using Eventso.Subscription.Pipeline;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Scrutor;

namespace Eventso.Subscription.Http.Hosting
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddSubscriptions(
            this IServiceCollection services,
            Action<ISubscriptionCollection, IServiceProvider> subscriptions,
            Func<ITypeSourceSelector, IImplementationTypeSelector> handlersSelector = null,
            ServiceLifetime handlersLifetime = ServiceLifetime.Scoped)
        {
            TryAddSubscriptionServices(services);

            services.AddSingleton(
                s =>
                {
                    var collection = new SubscriptionCollection();
                    subscriptions(collection, s);
                    return collection;
                });

            services
                .AddSingleton<ISubscriptionCollection>(s => s.GetRequiredService<SubscriptionCollection>())
                .AddSingleton<ISubscriptionConfigurationRegistry>(s => s.GetRequiredService<SubscriptionCollection>());

            if (handlersSelector != null)
            {
                services
                    .Scan(typeSourceSelector => handlersSelector(typeSourceSelector)
                    .AddClasses(typeFilter => typeFilter.AssignableTo(typeof(IMessageHandler<>)))
                    .UsingRegistrationStrategy(new IgnoreDuplicateRegistrationStrategy())
                    .AsImplementedInterfaces()
                    .WithLifetime(handlersLifetime));
            }

            return services;
        }

        private static void TryAddSubscriptionServices(IServiceCollection services)
        {
            services.TryAddSingleton<IMessagePipelineFactory, MessagePipelineFactory>();
            services.TryAddSingleton<IMessageHandlersRegistry, MessageHandlersRegistry>();
        }

        private sealed class IgnoreDuplicateRegistrationStrategy : RegistrationStrategy
        {
            public override void Apply(IServiceCollection services, ServiceDescriptor descriptor)
            {
                if (services.Any(serviceDescriptor =>
                    serviceDescriptor.ServiceType == descriptor.ServiceType
                    && GetImplementationType(serviceDescriptor) == GetImplementationType(descriptor)))
                {
                    return;
                }

                services.Add(descriptor);
            }

            private static Type GetImplementationType(ServiceDescriptor descriptor)
            {
                if (descriptor.ImplementationType != null)
                    return descriptor.ImplementationType;

                if (descriptor.ImplementationInstance != null)
                    return descriptor.ImplementationInstance.GetType();

                if (descriptor.ImplementationFactory == null)
                    return null;
                
                var typeArguments = descriptor.ImplementationFactory.GetType().GenericTypeArguments;
                return typeArguments[1];

            }
        }
    }
}