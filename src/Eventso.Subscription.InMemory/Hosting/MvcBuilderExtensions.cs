using Microsoft.Extensions.DependencyInjection;

namespace Eventso.Subscription.InMemory.Hosting
{
    public static class MvcBuilderExtensions
    {
        public static IMvcBuilder AddInMemoryBusController(this IMvcBuilder builder)
        {
            builder.AddApplicationPart(typeof(InMemoryBusController).Assembly);

            return builder;
        }
    }
}