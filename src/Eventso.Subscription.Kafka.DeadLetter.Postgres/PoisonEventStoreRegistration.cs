namespace Eventso.Subscription.Kafka.DeadLetter.Postgres;

public static class PoisonEventStoreRegistration
{
    // TODO uncomment later
    // public static IDeadLetterQueueOptions UsePostgresStore(
    //     this IDeadLetterQueueOptions options,
    //     IConnectionFactory connectionFactory)
    // {
    //
    //     var store = PoisonEventStore.Initialize(database.ConnectionFactory).GetAwaiter().GetResult(); 
    //     options.KafkaListener.Services.Replace(
    // //         ServiceDescriptor.Singleton<IPoisonEventStore>(store));

    //     return options;
    // }
}