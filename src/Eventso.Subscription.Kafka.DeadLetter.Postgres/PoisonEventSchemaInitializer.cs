using Npgsql;

namespace Eventso.Subscription.Kafka.DeadLetter.Postgres;

public static class PoisonEventSchemaInitializer
{
    public const string Sql =
        // language=sql
        """
        CREATE SCHEMA IF NOT EXISTS eventso_dlq;

        CREATE TABLE IF NOT EXISTS eventso_dlq.poison_events (
            group_id               TEXT           NOT NULL,
            topic                  TEXT           NOT NULL,
            partition              INT            NOT NULL,
            "offset"               BIGINT         NOT NULL,
            key                    BYTEA          NOT NULL,
            value                  BYTEA          NULL,
            creation_timestamp     TIMESTAMPTZ    NOT NULL,
            header_keys            TEXT[]         NULL,
            header_values          BYTEA[]        NULL,
            last_failure_timestamp TIMESTAMPTZ    NOT NULL,
            last_failure_reason    TEXT           NOT NULL,
            total_failure_count    INT            NOT NULL,
            lock_timestamp         TIMESTAMPTZ    NULL,
            update_timestamp       TIMESTAMPTZ    NOT NULL,
            delete_mark            BOOL           NULL,
            PRIMARY KEY ("offset", partition, topic, group_id)
        );

        CREATE INDEX IF NOT EXISTS ix_poison_events_key ON eventso_dlq.poison_events (key);
        """;

    public static async Task Initialize(IConnectionFactory connectionFactory, CancellationToken token)
    {
        await using var connection = connectionFactory.ReadWrite();

        await using var command = new NpgsqlCommand(Sql, connection);

        await connection.OpenAsync(token);

        await command.ExecuteNonQueryAsync(token);
    }
}