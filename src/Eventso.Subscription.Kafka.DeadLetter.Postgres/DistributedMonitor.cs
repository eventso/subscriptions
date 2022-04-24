using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace Eventso.Subscription.Kafka.DeadLetter.Postgres
{
    public static class DistributedMonitor
    {
        public static async Task<bool> TryEnter(NpgsqlConnection connection, long lockId, CancellationToken token)
        {
            var command = new NpgsqlCommand("SELECT pg_try_advisory_lock(@lockId);", connection)
            {
                Parameters = { new NpgsqlParameter<long>("lockId", lockId) }
            };

            await connection.OpenAsync(token);
            var result = await command.ExecuteScalarAsync(token);

            return result != null && (bool)result;
        }

        public static async Task Exit(NpgsqlConnection connection, long lockId, CancellationToken token)
        {
            var command = new NpgsqlCommand("SELECT pg_advisory_unlock(@lockId);", connection)
            {
                Parameters = { new NpgsqlParameter<long>("lockId", lockId) }
            };

            await connection.OpenAsync(token);
            await command.ExecuteNonQueryAsync(token);
        }
    }
}