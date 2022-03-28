using System;
using System.Collections.Generic;
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

        public PoisonEventStore(
            IConnectionFactory connectionFactory)
        {
            _connectionFactory = connectionFactory;
        }

        public async Task Add(StoredEvent @event, StoredFailure failure, CancellationToken cancellationToken)
        {
            await using var connection = _connectionFactory.ReadWrite();

            var headerKeys = new string[@event.Headers.Count];
            var headerValues = new ReadOnlyMemory<byte>[@event.Headers.Count];

            var index = 0;
            foreach (var header in @event.Headers)
            {
                headerKeys[index] = header.Key;
                headerValues[index] = header.Data;
                index++;
            }

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
                Parameters =
                {
                    new NpgsqlParameter<string>("topic", @event.TopicPartitionOffset.Topic),
                    new NpgsqlParameter<int>("partition", @event.TopicPartitionOffset.Partition.Value),
                    new NpgsqlParameter<long>("offset", @event.TopicPartitionOffset.Offset.Value),
                    new NpgsqlParameter<Guid>("key", @event.Key),
                    new NpgsqlParameter<ReadOnlyMemory<byte>>("value", @event.Value),
                    new NpgsqlParameter<DateTime>("creationTimestamp", @event.CreationTimestamp),
                    new NpgsqlParameter<string[]>("headerKeys", headerKeys),
                    new NpgsqlParameter<ReadOnlyMemory<byte>[]>("headerValues", headerValues),
                    new NpgsqlParameter<DateTime>("lastFailureTimestamp", failure.Timestamp),
                    new NpgsqlParameter<string>("lastFailureReason", failure.Reason),
                    new NpgsqlParameter<int>("totalFailureCount", 1),
                }
            };

            await connection.OpenAsync(cancellationToken);

            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        public async Task AddFailure(
            TopicPartitionOffset topicPartitionOffset,
            StoredFailure failure,
            CancellationToken cancellationToken)
        {
            await using var connection = _connectionFactory.ReadWrite();

            await using var command = new NpgsqlCommand(
                @"
UPDATE eventso_dlq.poison_events
SET
    last_failure_timestamp = @timestamp,
    last_failure_reason = @reason,
    total_failure_count = total_failure_count + 1
WHERE topic = @topic AND partition = @partition AND ""offset"" = @offset;",
                connection)
            {
                Parameters =
                {
                    new NpgsqlParameter<string>("topic", topicPartitionOffset.Topic),
                    new NpgsqlParameter<int>("partition", topicPartitionOffset.Partition.Value),
                    new NpgsqlParameter<long>("offset", topicPartitionOffset.Offset.Value),
                    new NpgsqlParameter<DateTime>("timestamp", failure.Timestamp),
                    new NpgsqlParameter<string>("reason", failure.Reason)
                }
            };

            await connection.OpenAsync(cancellationToken);

            await command.ExecuteNonQueryAsync(cancellationToken);
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

        public async Task<bool> Any(string topic, Guid key, CancellationToken cancellationToken)
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

        public async Task Remove(TopicPartitionOffset topicPartitionOffset, CancellationToken cancellationToken)
        {
            await using var connection = _connectionFactory.ReadWrite();

            await using var command = new NpgsqlCommand(
                "DELETE FROM eventso_dlq.poison_events WHERE topic = @topic AND partition = @partition AND \"offset\" = @offset;",
                connection)
            {
                Parameters =
                {
                    new NpgsqlParameter<string>("topic", topicPartitionOffset.Topic),
                    new NpgsqlParameter<int>("partition", topicPartitionOffset.Partition.Value),
                    new NpgsqlParameter<long>("offset", topicPartitionOffset.Offset.Value),
                }
            };

            await connection.OpenAsync(cancellationToken);

            await command.ExecuteNonQueryAsync(cancellationToken);
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
                    ? new StoredEventHeader[headerKeys.Length]
                    : Array.Empty<StoredEventHeader>();
                for (var i = 0; i < headerKeys.Length; i++)
                    headers[i] = new StoredEventHeader(headerKeys[i], headerValues[i]);

                yield return new StoredPoisonEvent(
                    new StoredEvent(
                        new TopicPartitionOffset(topic, reader.GetFieldValue<int>(0), reader.GetFieldValue<long>(1)),
                        reader.GetGuid(2),
                        reader.GetFieldValue<byte[]>(3),
                        reader.GetDateTime(4),
                        headers),
                    new StoredFailure(reader.GetDateTime(7), reader.GetString(8)),
                    reader.GetInt32(9)
                );
            }
        }
    }
}