using Microsoft.Extensions.DependencyInjection;

namespace Eventso.Subscription.WebApi.Hosting
{
    public static class MvcBuilderExtensions
    {
        public static IMvcBuilder AddConsumerController(this IMvcBuilder builder)
        {
            builder.AddApplicationPart(typeof(ConsumerController).Assembly);

            return builder;
        }
    }
}