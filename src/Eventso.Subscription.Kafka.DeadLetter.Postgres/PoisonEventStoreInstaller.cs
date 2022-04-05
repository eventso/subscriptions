using System.Threading.Tasks;
using Npgsql;

namespace Eventso.Subscription.Kafka.DeadLetter.Postgres
{
    public static class PoisonEventStoreInstaller
    {
        public static async Task Install(IConnectionFactory connectionFactory)
        {
            await using var connection = connectionFactory.ReadWrite();
            
            await using var command = new NpgsqlCommand(@"
                CREATE SCHEMA IF NOT EXISTS eventso_dlq;

                CREATE TABLE IF NOT EXISTS eventso_dlq.poison_events (
                    topic                  TEXT         NOT NULL,
                    partition              INT          NOT NULL,
                    ""offset""               BIGINT       NOT NULL,
                    key                    UUID         NOT NULL,
                    value                  BYTEA        NULL,
                    creation_timestamp     TIMESTAMP    NOT NULL,
                    header_keys            TEXT[]       NULL,
                    header_values          BYTEA[]      NULL,
                    last_failure_timestamp TIMESTAMP    NOT NULL,
                    last_failure_reason    TEXT         NOT NULL,
                    total_failure_count    INT          NOT NULL,
                    PRIMARY KEY (""offset"", partition, topic)
                );

                CREATE INDEX IF NOT EXISTS ix_poison_events_topic ON eventso_dlq.poison_events (topic);",
                connection);

            await connection.OpenAsync();
            
            await command.ExecuteNonQueryAsync();
        }
    }
}