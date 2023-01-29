using Microsoft.Extensions.DependencyInjection;

namespace Eventso.Subscription.Http.Hosting;

public static class MvcBuilderExtensions
{
    public static IMvcBuilder AddConsumerController(this IMvcBuilder builder)
    {
        builder.AddApplicationPart(typeof(SubscriptionController).Assembly);

        return builder;
    }
}