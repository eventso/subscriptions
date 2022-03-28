using Eventso.Subscription.Hosting;
using Eventso.Subscription.Kafka.DeadLetter.Store;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Eventso.Subscription.Kafka.DeadLetter.Postgres
{
    public static class PoisonEventStoreRegistration
    {
        // TODO uncomment later
        // public static IDeadLetterQueueOptions UsePostgresStore(
        //     this IDeadLetterQueueOptions options,
        //     IConnectionFactory connectionFactory)
        // {
        //     options.KafkaListener.Services.Replace(
        //         ServiceDescriptor.Singleton<IPoisonEventStore>(new PoisonEventStore(connectionFactory)));
        //
        //     return options;
        // }
    }
}