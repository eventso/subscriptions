using System;
using System.Threading;
using System.Threading.Tasks;
using Eventso.Subscription.Kafka.DeadLetter.Postgres;
using Npgsql;
using NSubstitute;
using Xunit;

namespace Eventso.Subscription.Tests
{
    public class PoisonEventStoreTests
    {
        [Fact]
        public async Task InitializingStore_StoreInitialized()
        {
            await using var database = await Database.Create(); 

            await PoisonEventStore.Install(database.ConnectionFactory);

            await using var connection = database.ConnectionFactory.ReadWrite();

            await using var command = new NpgsqlCommand(
                $"SELECT 1 FROM eventso_dlq.poison_events;",
                connection);

            await connection.OpenAsync();
            await command.ExecuteNonQueryAsync();
        }

        // to be continued

        private sealed class Database : IAsyncDisposable
        {
            private const string ConnectionStringFormat = "Host=localhost;Port=5432;Username=postgres;Password=postgres;Database={0};";
            private static int _uniqueness = 1;

            private readonly string _databaseName;

            private Database(string databaseName)
            {
                _databaseName = databaseName;

                var connectionString = string.Format(ConnectionStringFormat, $"{databaseName}");
                ConnectionFactory = Substitute.For<IConnectionFactory>();
                ConnectionFactory.ReadOnly().Returns(_ => new NpgsqlConnection(connectionString));
                ConnectionFactory.ReadWrite().Returns(_ => new NpgsqlConnection(connectionString));
            }

            public IConnectionFactory ConnectionFactory { get; }

            public static async Task<Database> Create()
            {
                var databaseName = $"pes_test_{DateTime.UtcNow:yyyyMMddHHmmss}_{Interlocked.Increment(ref _uniqueness)}";

                await using var connection = new NpgsqlConnection(CreateCommonConnectionString());

                await using var command = new NpgsqlCommand($"CREATE DATABASE {databaseName};", connection);

                await connection.OpenAsync();
                await command.ExecuteNonQueryAsync();

                return new Database(databaseName);
            }

            public async ValueTask DisposeAsync()
            {
                await using var connection = new NpgsqlConnection(CreateCommonConnectionString());

                await using var command = new NpgsqlCommand($@"
                REVOKE CONNECT ON DATABASE {_databaseName} FROM public;
                SELECT pid, pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{_databaseName}';
                DROP DATABASE {_databaseName};", connection);

                await connection.OpenAsync();
                await command.ExecuteNonQueryAsync();
            }

            private static string CreateCommonConnectionString()
                => string.Format(ConnectionStringFormat, "postgres");
        }
    }
}