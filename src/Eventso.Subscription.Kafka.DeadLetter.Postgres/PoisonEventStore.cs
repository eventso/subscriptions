using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Eventso.Subscription.Kafka.DeadLetter.Store;
using Npgsql;

namespace Eventso.Subscription.Kafka.DeadLetter.Postgres
{
    public sealed class PoisonEventStore : IPoisonEventStore
    {
        private readonly IConnectionFactory _connectionFactory;
        private readonly int _maxAllowedFailureCount;
        private readonly TimeSpan _minIntervalBetweenRetries;

        public PoisonEventStore(
            IConnectionFactory connectionFactory,
            // TODO receive from settings
            int? maxAllowedFailureCount = default,
            TimeSpan? minIntervalBetweenRetries = default)
        {
            _connectionFactory = connectionFactory;
            _maxAllowedFailureCount = maxAllowedFailureCount ?? 10;
            _minIntervalBetweenRetries = minIntervalBetweenRetries ?? TimeSpan.FromMinutes(1);
        }

        public static async Task<PoisonEventStore> Initialize(
            IConnectionFactory connectionFactory,
            // TODO receive from settings
            int? maxAllowedFailureCount = default,
            TimeSpan? minIntervalBetweenRetries = default,
            CancellationToken token = default)
        {
            await using var connection = connectionFactory.ReadWrite();
            
            await using var command = new NpgsqlCommand(@"
                CREATE SCHEMA IF NOT EXISTS eventso_dlq;

                CREATE TABLE IF NOT EXISTS eventso_dlq.poison_events (
                    topic                  TEXT         NOT NULL,
                    partition              INT          NOT NULL,
                    ""offset""             BIGINT       NOT NULL,
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

                CREATE INDEX IF NOT EXISTS ix_poison_events_key ON eventso_dlq.poison_events (key);",
                connection);

            await connection.OpenAsync(token);
            
            await command.ExecuteNonQueryAsync(token);

            return new PoisonEventStore(connectionFactory);
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

        public async Task<bool> IsStreamStored(string topic, Guid key, CancellationToken token)
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

            await connection.OpenAsync(token);

            var result = await command.ExecuteScalarAsync(token);
            return result != null && (bool)result;
        }

        public async IAsyncEnumerable<StreamId> GetStoredStreams(
            IReadOnlyCollection<StreamId> streamIds,
            [EnumeratorCancellation] CancellationToken token)
        {
            await using var connection = _connectionFactory.ReadOnly();

            var topics = new string[streamIds.Count];
            var keys = new Guid[streamIds.Count];
            foreach (var (index, streamId) in streamIds.Select((x, i) => (i, x)))
            {
                topics[index] = streamId.Topic;
                keys[index] = streamId.Key;
            }

            await using var command = new NpgsqlCommand(
                @"
SELECT DISTINCT pe.topic, pe.key
FROM eventso_dlq.poison_events pe
INNER JOIN UNNEST(@topics, @keys) AS input(topic, key) 
ON pe.topic = input.topic AND pe.key = input.key;",
                connection)
            {
                Parameters =
                {
                    new NpgsqlParameter<string[]>("topics", topics),
                    new NpgsqlParameter<Guid[]>("keys", keys)
                }
            };

            await connection.OpenAsync(token);

            var reader = await command.ExecuteReaderAsync(token);
            while (await reader.ReadAsync(token))
                yield return new StreamId(reader.GetString(0), reader.GetGuid(1));
        }

        public async Task Add(DateTime timestamp, IReadOnlyCollection<OpeningPoisonEvent> events, CancellationToken token)
        {
            await using var connection = _connectionFactory.ReadWrite();

            var topicParameter = new NpgsqlParameter<string> { ParameterName = "topic" };
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
    1);",
                connection)
            {
                Parameters = {
                    topicParameter,
                    partitionParameter,
                    offsetParameter,
                    keyParameter,
                    valueParameter,
                    creationTimestampParameter,
                    headerKeysParameter,
                    headerValuesParameter,
                    new NpgsqlParameter<DateTime>("lastFailureTimestamp", timestamp),
                    lastFailureReasonParameter
                }
            };

            await connection.OpenAsync(token);
            await command.PrepareAsync(token);

            foreach (var poisonEvent in events)
            {
                var headerKeys = new string[poisonEvent.Headers.Count];
                var headerValues = new ReadOnlyMemory<byte>[poisonEvent.Headers.Count];

                foreach (var (index, header) in poisonEvent.Headers.Select((x, i) => (i, x)))
                {
                    headerKeys[index] = header.Key;
                    headerValues[index] = header.Data;
                }

                topicParameter.TypedValue = poisonEvent.TopicPartitionOffset.Topic;
                partitionParameter.TypedValue = poisonEvent.TopicPartitionOffset.Partition.Value;
                offsetParameter.TypedValue = poisonEvent.TopicPartitionOffset.Offset.Value;
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
            DateTime timestamp,
            IReadOnlyCollection<OccuredFailure> failures,
            CancellationToken token)
        {
            var topics = new string[failures.Count];
            var partitions = new int[failures.Count];
            var offsets = new long[failures.Count];
            var reasons = new string[failures.Count];
            foreach (var (index, failure) in failures.Select((x, i) => (i, x)))
            {
                topics[index] = failure.TopicPartitionOffset.Topic;
                partitions[index] = failure.TopicPartitionOffset.Partition.Value;
                offsets[index] = failure.TopicPartitionOffset.Offset.Value;
                reasons[index] = failure.Reason;
            }
            
            await using var connection = _connectionFactory.ReadWrite();

            await using var command = new NpgsqlCommand(
                @"
UPDATE eventso_dlq.poison_events pe
SET
    last_failure_timestamp = @timestamp,
    last_failure_reason = input.reason,
    total_failure_count = total_failure_count + 1
FROM UNNEST(@topics, @partitions, @offsets, @reasons) AS input(topic, partition, ""offset"", reason)
WHERE pe.topic = input.topic AND pe.partition = input.partition AND pe.""offset"" = input.""offset"";",
                connection)
            {
                Parameters =
                {
                    new NpgsqlParameter<string[]>("topics", topics),
                    new NpgsqlParameter<int[]>("partitions", partitions),
                    new NpgsqlParameter<long[]>("offsets", offsets),
                    new NpgsqlParameter<DateTime>("timestamp", timestamp),
                    new NpgsqlParameter<string[]>("reasons", reasons)
                }
            };

            await connection.OpenAsync(token);

            await command.ExecuteNonQueryAsync(token);
        }

        public async Task Remove(IReadOnlyCollection<TopicPartitionOffset> topicPartitionOffsets, CancellationToken token)
        {
            var topics = new string[topicPartitionOffsets.Count];
            var partitions = new int[topicPartitionOffsets.Count];
            var offsets = new long[topicPartitionOffsets.Count];
            foreach (var (index, topicPartitionOffset) in topicPartitionOffsets.Select((x, i) => (i, x)))
            {
                topics[index] = topicPartitionOffset.Topic;
                partitions[index] = topicPartitionOffset.Partition.Value;
                offsets[index] = topicPartitionOffset.Offset.Value;
            }

            await using var connection = _connectionFactory.ReadWrite();

            await using var command = new NpgsqlCommand(
                @"
DELETE FROM eventso_dlq.poison_events pe
USING UNNEST (@topics, @partitions, @offsets) AS input(topic, partition, ""offset"")
WHERE pe.topic = input.topic AND pe.partition = input.partition AND pe.""offset"" = input.""offset"";",
                connection)
            {
                Parameters =
                {
                    new NpgsqlParameter<string[]>("topics", topics),
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
    AND pe_heads.last_failure_timestamp < @maxAcceptedLastFailureTimestamp;",
                connection)
            {
                Parameters =
                {
                    new NpgsqlParameter<string>("topic", topic),
                    new NpgsqlParameter<int>("maxAllowedFailureCount", _maxAllowedFailureCount),
                    new NpgsqlParameter<DateTime>("maxAcceptedLastFailureTimestamp", DateTime.UtcNow - _minIntervalBetweenRetries)
                }
            };

            await connection.OpenAsync(cancellationToken);

            var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                yield return new StoredPoisonEvent(
                    reader.GetFieldValue<int>(0),
                    reader.GetFieldValue<long>(1),
                    reader.GetGuid(2),
                    reader.GetFieldValue<byte[]>(3),
                    reader.GetDateTime(4),
                    ReadEventHeaders(),
                    reader.GetDateTime(7),
                    reader.GetString(8),
                    reader.GetInt32(9)
                );

                EventHeader[] ReadEventHeaders()
                {
                    var headerKeys = reader.GetFieldValue<string[]>(5);
                    if (headerKeys.Length <= 0)
                        return Array.Empty<EventHeader>();

                    var headerValues = reader.GetFieldValue<byte[][]>(6);
                    var headers = new EventHeader[headerKeys.Length];
                    for (var i = 0; i < headerKeys.Length; i++)
                        headers[i] = new EventHeader(headerKeys[i], headerValues[i]);
                    return headers;
                }
            }
        }
    }
}