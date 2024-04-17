using System.Runtime.CompilerServices;
using Confluent.Kafka;
using Eventso.Subscription.Kafka.DeadLetter.Store;
using Npgsql;

namespace Eventso.Subscription.Kafka.DeadLetter.Postgres;

internal sealed class PoisonEventStore : IPoisonEventStore
{
    private readonly IConnectionFactory _connectionFactory;
    private readonly int _maxAllowedFailureCount;
    private readonly TimeSpan _minIntervalBetweenRetries;
    private readonly TimeSpan _maxLockHandleInterval;

    public PoisonEventStore(
        IConnectionFactory connectionFactory,
        int? maxAllowedFailureCount,
        TimeSpan? minIntervalBetweenRetries,
        TimeSpan? maxLockHandleInterval)
    {
        _connectionFactory = connectionFactory;
        _maxAllowedFailureCount = maxAllowedFailureCount ?? 10;
        _minIntervalBetweenRetries = minIntervalBetweenRetries ?? TimeSpan.FromMinutes(1);
        _maxLockHandleInterval = maxLockHandleInterval ?? TimeSpan.FromMinutes(1);
    }

    public static async Task<PoisonEventStore> Initialize(
        IConnectionFactory connectionFactory,
        int? maxAllowedFailureCount,
        TimeSpan? minIntervalBetweenRetries,
        TimeSpan? maxLockHandleInterval,
        CancellationToken token)
    {
        await using var connection = connectionFactory.ReadWrite();
            
        await using var command = new NpgsqlCommand(@"
                CREATE SCHEMA IF NOT EXISTS eventso_dlq;

                CREATE TABLE IF NOT EXISTS eventso_dlq.poison_events (
                    group_id               TEXT         NOT NULL,
                    topic                  TEXT         NOT NULL,
                    partition              INT          NOT NULL,
                    ""offset""             BIGINT       NOT NULL,
                    key                    BYTEA        NOT NULL,
                    value                  BYTEA        NULL,
                    creation_timestamp     TIMESTAMP    NOT NULL,
                    header_keys            TEXT[]       NULL,
                    header_values          BYTEA[]      NULL,
                    last_failure_timestamp TIMESTAMP    NOT NULL,
                    last_failure_reason    TEXT         NOT NULL,
                    total_failure_count    INT          NOT NULL,
                    lock_timestamp         TIMESTAMP    NULL,
                    PRIMARY KEY (""offset"", partition, topic, group_id)
                );

                CREATE INDEX IF NOT EXISTS ix_poison_events_key ON eventso_dlq.poison_events (key);",
            connection);

        await connection.OpenAsync(token);
            
        await command.ExecuteNonQueryAsync(token);

        return new PoisonEventStore(
            connectionFactory,
            maxAllowedFailureCount,
            minIntervalBetweenRetries,
            maxLockHandleInterval);
    }

    public async Task<long> CountPoisonedEvents(string groupId, string topic, CancellationToken cancellationToken)
    {
        await using var connection = _connectionFactory.ReadOnly();

        await using var command = new NpgsqlCommand(
            "SELECT COUNT(*) FROM eventso_dlq.poison_events WHERE topic = @topic AND group_id = @groupId;",
            connection)
        {
            Parameters =
            {
                new NpgsqlParameter<string>("topic", topic),
                new NpgsqlParameter<string>("groupId", groupId)
            }
        };

        await connection.OpenAsync(cancellationToken);

        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result != null ? (long)result : 0;
    }

    public async Task<bool> IsKeyPoisoned(string groupId, string topic, ReadOnlyMemory<byte> key, CancellationToken token)
    {
         await using var connection = _connectionFactory.ReadOnly();

         await using var command = new NpgsqlCommand(
             "SELECT TRUE FROM eventso_dlq.poison_events WHERE topic = @topic AND key = @key AND group_id = @groupId LIMIT 1;",
             connection)
         {
             Parameters =
             {
                 new NpgsqlParameter<string>("topic", topic),
                 new NpgsqlParameter<ReadOnlyMemory<byte>>("key", key),
                 new NpgsqlParameter<string>("groupId", groupId)
             }
         };

         await connection.OpenAsync(token);

         var result = await command.ExecuteScalarAsync(token);
         return result != null && (bool)result;
    }

    public async IAsyncEnumerable<ReadOnlyMemory<byte>> GetPoisonedKeys(
        string groupId,
        TopicPartition topicPartition,
        [EnumeratorCancellation] CancellationToken token)
    {
        await using var connection = _connectionFactory.ReadOnly();

        await using var command = new NpgsqlCommand(
            @"
SELECT DISTINCT key
FROM eventso_dlq.poison_events
WHERE topic = @topic AND partition = @partition AND group_id = @groupId;",
            connection)
        {
            Parameters =
            {
                new NpgsqlParameter<string>("topic", topicPartition.Topic),
                new NpgsqlParameter<int>("partition", topicPartition.Partition.Value),
                new NpgsqlParameter<string>("groupId", groupId)
            }
        };

        await connection.OpenAsync(token);

        await using var reader = await command.ExecuteReaderAsync(token);
        while (await reader.ReadAsync(token))
            yield return reader.GetFieldValue<byte[]>(0);
    }

    public async Task AddEvent(
        string groupId,
        PoisonEvent @event,
        DateTime timestamp,
        string reason,
        CancellationToken token)
    {
        await using var connection = _connectionFactory.ReadWrite();

        var headerKeys = new string[@event.Headers.Count];
        var headerValues = new ReadOnlyMemory<byte>[@event.Headers.Count];

        foreach (var (index, header) in @event.Headers.Select((x, i) => (i, x)))
        {
            headerKeys[index] = header.Key;
            headerValues[index] = header.Data;
        }

        await using var command = new NpgsqlCommand(
            @"
INSERT INTO eventso_dlq.poison_events(
    topic,
    partition,
    ""offset"",
    group_id,
    key,
    value,
    creation_timestamp,
    header_keys,
    header_values,
    last_failure_timestamp,
    last_failure_reason,
    total_failure_count)
VALUES (
    @topic,
    @partition,
    @offset,
    @groupId,
    @key,
    @value,
    @creationTimestamp,
    @headerKeys,
    @headerValues,
    @lastFailureTimestamp,
    @lastFailureReason,
    @totalFailureCount)
ON CONFLICT (""offset"", partition, topic, group_id)
DO UPDATE
SET
    last_failure_timestamp = excluded.last_failure_timestamp,
    last_failure_reason = excluded.last_failure_reason,
    total_failure_count = excluded.total_failure_count,
    lock_timestamp = NULL
WHERE poison_events.total_failure_count < excluded.total_failure_count;",
            connection)
        {
            Parameters = {
                new NpgsqlParameter<string>("topic", @event.TopicPartitionOffset.Topic),
                new NpgsqlParameter<int>("partition", @event.TopicPartitionOffset.Partition.Value),
                new NpgsqlParameter<long>("offset", @event.TopicPartitionOffset.Offset.Value),
                new NpgsqlParameter<string>("groupId", groupId),
                new NpgsqlParameter<ReadOnlyMemory<byte>>("key", @event.Key),
                new NpgsqlParameter<ReadOnlyMemory<byte>>("value", @event.Value),
                new NpgsqlParameter<DateTime>("creationTimestamp", @event.CreationTimestamp),
                new NpgsqlParameter<string[]>("headerKeys", headerKeys),
                new NpgsqlParameter<ReadOnlyMemory<byte>[]>("headerValues", headerValues),
                new NpgsqlParameter<DateTime>("lastFailureTimestamp", timestamp),
                new NpgsqlParameter<string>("lastFailureReason", reason),
                new NpgsqlParameter<int>("totalFailureCount", @event.FailureCount)
            }
        };

        await connection.OpenAsync(token);

        await command.ExecuteNonQueryAsync(token);
    }

    public async Task RemoveEvent(string groupId, TopicPartitionOffset partitionOffset, CancellationToken token)
    {
        await using var connection = _connectionFactory.ReadWrite();

        await using var command = new NpgsqlCommand(
            @"
DELETE FROM eventso_dlq.poison_events pe
WHERE pe.topic = @topic AND pe.partition = @partition AND pe.""offset"" = @offset AND group_id = @groupId;",
            connection)
        {
            Parameters =
            {
                new NpgsqlParameter<string>("topic", partitionOffset.Topic),
                new NpgsqlParameter<int>("partition", partitionOffset.Partition.Value),
                new NpgsqlParameter<long>("offset", partitionOffset.Offset.Value),
                new NpgsqlParameter<string>("groupId", groupId),
            }
        };

        await connection.OpenAsync(token);

        await command.ExecuteNonQueryAsync(token);
    }

    public async Task<PoisonEvent?> GetEventForRetrying(string groupId, TopicPartition topicPartition, CancellationToken token)
    {
        await using var connection = _connectionFactory.ReadOnly();

        await connection.OpenAsync(token);

        await using var command = new NpgsqlCommand(
            @"
WITH
    heads AS (
        SELECT key, MIN(""offset"") AS min_offset
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
            AND pe_heads.""offset"" = heads.min_offset
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
WHERE pe.topic = le.topic AND pe.partition = le.partition AND pe.""offset"" = le.""offset"" AND pe.group_id = le.group_id
RETURNING 
    pe.""offset"",
    pe.key,
    pe.value,
    pe.creation_timestamp,
    pe.header_keys,
    pe.header_values,
    pe.total_failure_count,
    pe.lock_timestamp;",
            connection)
        {
            Parameters =
            {
                new NpgsqlParameter<string>("topic", topicPartition.Topic),
                new NpgsqlParameter<int>("partition", topicPartition.Partition),
                new NpgsqlParameter<string>("groupId", groupId),
                new NpgsqlParameter<int>("maxFailureCount", _maxAllowedFailureCount),
                new NpgsqlParameter<DateTime>("maxLastFailureTimestamp", DateTime.UtcNow - _minIntervalBetweenRetries),
                new NpgsqlParameter<DateTime>("maxLockTimestamp", DateTime.UtcNow - _maxLockHandleInterval)
            }
        };

        await using var reader = await command.ExecuteReaderAsync(token);
        if (!await reader.ReadAsync(token))
            return null;

        var poisonEvent = new PoisonEvent(
            new TopicPartitionOffset(topicPartition, reader.GetFieldValue<long>(0)),
            reader.GetFieldValue<byte[]>(1),
            reader.GetFieldValue<byte[]>(2),
            reader.GetDateTime(3),
            ReadEventHeaders(),
            reader.GetInt32(6));

        if (await reader.ReadAsync(token))
            throw new Exception("Unexpected additional row");

        return poisonEvent;

        EventHeader[] ReadEventHeaders()
        {
            var headerKeys = reader.GetFieldValue<string[]>(4);
            if (headerKeys.Length <= 0)
                return [];

            var headerValues = reader.GetFieldValue<byte[][]>(5);
            var headers = new EventHeader[headerKeys.Length];
            for (var i = 0; i < headerKeys.Length; i++)
                headers[i] = new EventHeader(headerKeys[i], headerValues[i]);
            return headers;
        }
    }
}