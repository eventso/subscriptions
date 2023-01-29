using Microsoft.Extensions.DependencyInjection;

namespace Eventso.Subscription.Kafka.Insights;

public static class ServiceCollectionExtensions
{
    public static IMvcBuilder AddKafkaInsights(this IMvcBuilder mvcBuilder)
    {
        if (mvcBuilder == null)
            throw new ArgumentNullException(nameof(mvcBuilder));

        mvcBuilder.AddApplicationPart(typeof(KafkaController).Assembly);

        return mvcBuilder;
    }
}