using Confluent.Kafka;
using Npgsql;

namespace Eventso.Subscription.Kafka.DeadLetter.Postgres;

internal sealed class PoisonEventRetryingScheduler(
    IConnectionFactory connectionFactory,
    int maxAllowedFailureCount,
    TimeSpan minIntervalBetweenRetries,
    TimeSpan maxLockHandleInterval)
    : IPoisonEventRetryingScheduler
{
    public async Task<PoisonEvent?> GetEventForRetrying(string groupId, TopicPartition topicPartition, CancellationToken token)
    {
        await using var connection = connectionFactory.ReadOnly();

        await connection.OpenAsync(token);

        await using var command = new NpgsqlCommand(
            // language=sql
            """
            WITH
                heads AS (
                    SELECT key, MIN("offset") AS min_offset
                    FROM eventso_dlq.poison_events
                    WHERE topic = @topic AND partition = @partition AND group_id = @groupId
                    GROUP BY key
                ),
                locked_events AS (
                    SELECT pe_heads.*
                    FROM eventso_dlq.poison_events pe_heads
                    INNER JOIN heads
                    ON pe_heads.topic = @topic
                        AND pe_heads.partition = @partition
                        AND pe_heads."offset" = heads.min_offset
                        AND pe_heads.key = heads.key
                        AND pe_heads.group_id = @groupId
                    WHERE
                        pe_heads.total_failure_count <= @maxFailureCount
                        AND pe_heads.last_failure_timestamp < @maxLastFailureTimestamp
                        AND (pe_heads.lock_timestamp IS NULL OR pe_heads.lock_timestamp < @maxLockTimestamp)
                    FOR UPDATE OF pe_heads SKIP LOCKED
                    LIMIT 1
                )
            UPDATE eventso_dlq.poison_events pe
            SET lock_timestamp = NOW()
            FROM locked_events le
            WHERE pe.topic = le.topic AND pe.partition = le.partition AND pe."offset" = le."offset" AND pe.group_id = le.group_id
            RETURNING
                pe."offset",
                pe.key,
                pe.value,
                pe.creation_timestamp,
                pe.header_keys,
                pe.header_values,
                pe.total_failure_count,
                pe.lock_timestamp;
            """,
            connection);

        command.Parameters.Add(new NpgsqlParameter<string>("topic", topicPartition.Topic));
        command.Parameters.Add(new NpgsqlParameter<int>("partition", topicPartition.Partition));
        command.Parameters.Add(new NpgsqlParameter<string>("groupId", groupId));
        command.Parameters.Add(new NpgsqlParameter<int>("maxFailureCount", maxAllowedFailureCount));
        command.Parameters.Add(new NpgsqlParameter<DateTime>("maxLastFailureTimestamp", DateTime.UtcNow - minIntervalBetweenRetries));
        command.Parameters.Add(new NpgsqlParameter<DateTime>("maxLockTimestamp", DateTime.UtcNow - maxLockHandleInterval));

        await using var reader = await command.ExecuteReaderAsync(token);
        if (!await reader.ReadAsync(token))
            return null;

        var poisonEvent = new PoisonEvent(
            new TopicPartitionOffset(topicPartition, reader.GetFieldValue<long>(0)),
            reader.GetFieldValue<byte[]>(1),
            reader.GetFieldValue<byte[]>(2),
            reader.GetDateTime(3),
            ReadHeaders(),
            reader.GetInt32(6));

        if (await reader.ReadAsync(token))
            throw new Exception("Unexpected additional row");

        return poisonEvent;

        PoisonEvent.Header[] ReadHeaders()
        {
            var headerKeys = reader.GetFieldValue<string[]>(4);
            if (headerKeys.Length <= 0)
                return [];

            var headerValues = reader.GetFieldValue<byte[][]>(5);
            var headers = new PoisonEvent.Header[headerKeys.Length];
            for (var i = 0; i < headerKeys.Length; i++)
                headers[i] = new PoisonEvent.Header(headerKeys[i], headerValues[i]);
            return headers;
        }
    }
}