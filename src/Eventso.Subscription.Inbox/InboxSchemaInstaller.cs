using System;
using System.Threading.Tasks;
using Npgsql;

namespace Eventso.Subscription.Inbox
{
    public sealed class InboxSchemaInstaller
    {
        private NpgsqlConnection _connection;

        public InboxSchemaInstaller(NpgsqlConnection connection)
        {
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        }

        public async Task InstallAsync(string schema)
        {
            using (var command = new NpgsqlCommand($@"
                CREATE SCHEMA IF NOT EXISTS ""{schema}"";

                CREATE TABLE IF NOT EXISTS ""{schema}"".messages (
                   ,stream_id    uuid         NOT NULL
                   ,number       bigint       NOT NULL
                   ,body         bytea        NOT NULL
                   ,error        text         NOT NULL
                   ,attempts,    int          NOT NULL DEFAULT 1
                   ,updated      timestamptz  NOT NULL DEFAULT now()
                   ,created      timestamptz  NOT NULL DEFAULT now()
                   ,PRIMARY KEY (stream_id, number)
                );",
                _connection))
            {
                await command.ExecuteNonQueryAsync();
            }
        }

        public void Dispose()
        {
            _connection?.Dispose();
            _connection = null;
        }
    }
}