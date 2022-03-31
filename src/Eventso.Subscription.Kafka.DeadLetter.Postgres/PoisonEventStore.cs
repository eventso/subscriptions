using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Eventso.Subscription.Kafka.DeadLetter.Store;
using Npgsql;

namespace Eventso.Subscription.Kafka.DeadLetter.Postgres
{
    public sealed class PoisonEventStore : IPoisonEventStore
    {
        private readonly IConnectionFactory _connectionFactory;

        public PoisonEventStore(
            IConnectionFactory connectionFactory)
        {
            _connectionFactory = connectionFactory;
        }

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

        public async Task<long> Count(string topic, CancellationToken cancellationToken)
        {
            await using var connection = _connectionFactory.ReadOnly();

            await using var command = new NpgsqlCommand(
                "SELECT COUNT(*) FROM eventso_dlq.poison_events WHERE topic = @topic;",
                connection)
            {
                Parameters =
                {
                    new NpgsqlParameter<string>("topic", topic)
                }
            };

            await connection.OpenAsync(cancellationToken);

            var result = await command.ExecuteScalarAsync(cancellationToken);
            return result != null ? (long)result : 0;
        }

        public async Task<bool> IsKeyStored(string topic, Guid key, CancellationToken cancellationToken)
        {
            await using var connection = _connectionFactory.ReadOnly();

            await using var command = new NpgsqlCommand(
                "SELECT TRUE FROM eventso_dlq.poison_events WHERE topic = @topic AND key = @key;",
                connection)
            {
                Parameters =
                {
                    new NpgsqlParameter<string>("topic", topic),
                    new NpgsqlParameter<Guid>("key", key)
                }
            };

            await connection.OpenAsync(cancellationToken);

            var result = await command.ExecuteScalarAsync(cancellationToken);
            return result != null && (bool)result;
        }

        public async IAsyncEnumerable<Guid> GetStoredKeys(
            string topic,
            IReadOnlyCollection<Guid> keys,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await using var connection = _connectionFactory.ReadOnly();

            await using var command = new NpgsqlCommand(
                "SELECT DISTINCT key FROM eventso_dlq.poison_events WHERE topic = @topic AND key = ANY(@keys);",
                connection)
            {
                Parameters =
                {
                    new NpgsqlParameter<string>("topic", topic),
                    new NpgsqlParameter<IReadOnlyCollection<Guid>>("keys", keys)
                }
            };

            await connection.OpenAsync(cancellationToken);

            var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
                yield return reader.GetGuid(0);
        }

        public async Task Add(string topic, DateTime timestamp, IReadOnlyCollection<OpeningPoisonEvent> poisonEvents, CancellationToken token)
        {
            await using var connection = _connectionFactory.ReadWrite();

            var partitionParameter = new NpgsqlParameter<int> { ParameterName = "partition" };
            var offsetParameter = new NpgsqlParameter<long> { ParameterName = "offset" };
            var keyParameter = new NpgsqlParameter<Guid> { ParameterName = "key" };
            var valueParameter = new NpgsqlParameter<ReadOnlyMemory<byte>> { ParameterName = "value" };
            var creationTimestampParameter = new NpgsqlParameter<DateTime> { ParameterName = "creationTimestamp" };
            var headerKeysParameter = new NpgsqlParameter<string[]> { ParameterName = "headerKeys" };
            var headerValuesParameter = new NpgsqlParameter<ReadOnlyMemory<byte>[]> { ParameterName = "headerValues" };
            var lastFailureReasonParameter = new NpgsqlParameter<string> { ParameterName = "lastFailureReason" };

            await using var command = new NpgsqlCommand(
                @"
INSERT INTO eventso_dlq.poison_events(
    topic,
    partition,
    ""offset"",
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
    @key,
    @value,
    @creationTimestamp,
    @headerKeys,
    @headerValues,
    @lastFailureTimestamp,
    @lastFailureReason,
    @totalFailureCount);",
                connection)
            {
                Parameters = {
                    new NpgsqlParameter<string>("topic", topic),
                    partitionParameter,
                    offsetParameter,
                    keyParameter,
                    valueParameter,
                    creationTimestampParameter,
                    headerKeysParameter,
                    headerValuesParameter,
                    new NpgsqlParameter<DateTime>("lastFailureTimestamp", timestamp),
                    lastFailureReasonParameter,
                    new NpgsqlParameter<int>("totalFailureCount", 1),
                }
            };

            await connection.OpenAsync(token);
            await command.PrepareAsync(token);

            foreach (var poisonEvent in poisonEvents)
            {
                var headerKeys = new string[poisonEvent.Headers.Count];
                var headerValues = new ReadOnlyMemory<byte>[poisonEvent.Headers.Count];

                foreach (var (index, header) in Enumerate(poisonEvent.Headers))
                {
                    headerKeys[index] = header.Key;
                    headerValues[index] = header.Data;
                }

                partitionParameter.TypedValue = poisonEvent.PartitionOffset.Partition.Value;
                offsetParameter.TypedValue = poisonEvent.PartitionOffset.Offset.Value;
                keyParameter.TypedValue = poisonEvent.Key;
                valueParameter.TypedValue = poisonEvent.Value;
                creationTimestampParameter.TypedValue = poisonEvent.CreationTimestamp;
                headerKeysParameter.TypedValue = headerKeys;
                headerValuesParameter.TypedValue = headerValues;
                lastFailureReasonParameter.TypedValue = poisonEvent.FailureReason;

                await command.ExecuteNonQueryAsync(token);
            }
        }

        public async Task AddFailures(
            string topic,
            DateTime timestamp,
            IReadOnlyCollection<RecentFailure> failures,
            CancellationToken token)
        {
            var partitions = new int[failures.Count];
            var offsets = new long[failures.Count];
            var reasons = new string[failures.Count];
            foreach (var (index, failure) in Enumerate(failures))
            {
                partitions[index] = failure.PartitionOffset.Partition.Value;
                offsets[index] = failure.PartitionOffset.Offset.Value;
                reasons[index] = failure.Reason;
            }
            
            await using var connection = _connectionFactory.ReadWrite();

            await using var command = new NpgsqlCommand(
                @"
UPDATE eventso_dlq.poison_events
SET
    last_failure_timestamp = @timestamp,
    last_failure_reason = @reason,
    total_failure_count = total_failure_count + 1
FROM UNNEST(@partitions, @offsets, @reasons) AS input(partition, ""offset"", reason)
WHERE topic = @topic AND partition = input.partition AND ""offset"" = input.""offset"";",
                connection)
            {
                Parameters =
                {
                    new NpgsqlParameter<string>("topic", topic),
                    new NpgsqlParameter<int[]>("partitions", partitions),
                    new NpgsqlParameter<long[]>("offsets", offsets),
                    new NpgsqlParameter<DateTime>("timestamp", timestamp),
                    new NpgsqlParameter<string[]>("reasons", reasons)
                }
            };

            await connection.OpenAsync(token);

            await command.ExecuteNonQueryAsync(token);
        }

        public async Task Remove(string topic, IReadOnlyCollection<PartitionOffset> partitionOffsets, CancellationToken token)
        {
            var partitions = new int[partitionOffsets.Count];
            var offsets = new long[partitionOffsets.Count];
            foreach (var (index, partitionOffset) in Enumerate(partitionOffsets))
            {
                partitions[index] = partitionOffset.Partition.Value;
                offsets[index] = partitionOffset.Offset.Value;
            }

            await using var connection = _connectionFactory.ReadWrite();

            await using var command = new NpgsqlCommand(
                @"
DELETE FROM eventso_dlq.poison_events pe
JOIN UNNEST (@partitions, @offsets) AS input(partition, ""offset"")
ON pe.partition = input.partition AND pe.""offset"" = input.""offset""
WHERE topic = @topic;",
                connection)
            {
                Parameters =
                {
                    new NpgsqlParameter<string>("topic", topic),
                    new NpgsqlParameter<int[]>("partitions", partitions),
                    new NpgsqlParameter<long[]>("offsets", offsets),
                }
            };

            await connection.OpenAsync(token);

            await command.ExecuteNonQueryAsync(token);
        }

        public async IAsyncEnumerable<StoredPoisonEvent> GetEventsForRetrying(
            string topic,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await using var connection = _connectionFactory.ReadOnly();

            await using var command = new NpgsqlCommand(
                @"

SELECT
    partition,
    ""offset"",
    pe_heads.key,
    value,
    creation_timestamp,
    header_keys,
    header_values,
    last_failure_timestamp,
    last_failure_reason,
    total_failure_count
FROM eventso_dlq.poison_events pe_heads
INNER JOIN (
    SELECT key, MIN(""offset"") AS min_offset
    FROM eventso_dlq.poison_events
    WHERE topic = @topic
    GROUP BY key
) heads
ON pe_heads.key = heads.key and pe_heads.""offset"" = heads.min_offset
WHERE
    pe_heads.total_failure_count <= @maxAllowedFailureCount
    AND NOW() - pe_heads.last_failure_timestamp > @minIntervalBetweenRetries;",
                connection)
            {
                Parameters =
                {
                    new NpgsqlParameter<string>("topic", topic),
                    new NpgsqlParameter<int>("maxAllowedFailureCount", 20), // TODO from settings
                    new NpgsqlParameter<TimeSpan>("minIntervalBetweenRetries", TimeSpan.FromMinutes(1))  // TODO from settings
                }
            };

            await connection.OpenAsync(cancellationToken);

            var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var headerKeys = reader.GetFieldValue<string[]>(5);
                var headerValues = reader.GetFieldValue<byte[][]>(6);

                var headers = headerKeys.Length > 0
                    ? new EventHeader[headerKeys.Length]
                    : Array.Empty<EventHeader>();
                for (var i = 0; i < headerKeys.Length; i++)
                    headers[i] = new EventHeader(headerKeys[i], headerValues[i]);

                yield return new StoredPoisonEvent(
                    new PartitionOffset(reader.GetFieldValue<int>(0), reader.GetFieldValue<long>(1)),
                    reader.GetGuid(2),
                    reader.GetFieldValue<byte[]>(3),
                    reader.GetDateTime(4),
                    headers,
                    reader.GetDateTime(7), reader.GetString(8),
                    reader.GetInt32(9)
                );
            }
        }

        private static IEnumerable<(int index, T value)> Enumerate<T>(IEnumerable<T> collection)
        {
            var i = 0;
            foreach (var item in collection)
            {
                yield return (i, item);
                i++;
            }
        }
    }
}