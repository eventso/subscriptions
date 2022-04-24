using Npgsql;

namespace Eventso.Subscription.Kafka.DeadLetter.Postgres
{
    public interface IConnectionFactory
    {
        NpgsqlConnection ReadWrite();
        NpgsqlConnection ReadOnly();
    }
}