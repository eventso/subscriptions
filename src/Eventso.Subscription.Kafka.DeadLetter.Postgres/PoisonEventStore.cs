using System.Runtime.CompilerServices;
using Confluent.Kafka;
using Npgsql;

namespace Eventso.Subscription.Kafka.DeadLetter.Postgres;

internal sealed class PoisonEventStore(IConnectionFactory connectionFactory) : IPoisonEventStore
{
    public async Task<IReadOnlyDictionary<ConsumingTopic, long>> CountPoisonedEvents(CancellationToken token)
    {
        await using var connection = connectionFactory.ReadOnly();

        await using var command = new NpgsqlCommand(
            """
            SELECT topic, group_id, COUNT(*)
            FROM eventso_dlq.poison_events
            GROUP BY topic, group_id
            ;
            """,
            connection);

        await connection.OpenAsync(token);

        var result = new Dictionary<ConsumingTopic, long>();
        await using var reader = await command.ExecuteReaderAsync(token);

        while (await reader.ReadAsync(token))
            result.Add(new ConsumingTopic(reader.GetString(0), reader.GetString(1)), reader.GetInt64(2));
        
        return result;
    }

    public async Task<long> CountPoisonedEvents(string groupId, string topic, CancellationToken cancellationToken)
    {
        await using var connection = connectionFactory.ReadOnly();

        await using var command = new NpgsqlCommand(
            "SELECT COUNT(*) FROM eventso_dlq.poison_events WHERE topic = @topic AND group_id = @groupId;",
            connection);
        command.Parameters.Add(new NpgsqlParameter<string>("topic", topic));
        command.Parameters.Add(new NpgsqlParameter<string>("groupId", groupId));

        await connection.OpenAsync(cancellationToken);

        var result = await command.ExecuteScalarAsync(cancellationToken);

        return result != null ? (long)result : 0;
    }

    public async Task<bool> IsKeyPoisoned(string groupId, string topic, byte[] key, CancellationToken token)
    {
         await using var connection = connectionFactory.ReadOnly();

         await using var command = new NpgsqlCommand(
             "SELECT TRUE FROM eventso_dlq.poison_events WHERE topic = @topic AND key = @key AND group_id = @groupId LIMIT 1;",
             connection);
         command.Parameters.Add(new NpgsqlParameter<string>("topic", topic));
         command.Parameters.Add(new NpgsqlParameter<ReadOnlyMemory<byte>>("key", key));
         command.Parameters.Add(new NpgsqlParameter<string>("groupId", groupId));

         await connection.OpenAsync(token);

         var result = await command.ExecuteScalarAsync(token);

         return result is not (null or DBNull) && (bool)result;
    }

    public async IAsyncEnumerable<byte[]> GetPoisonedKeys(
        string groupId,
        TopicPartition topicPartition,
        [EnumeratorCancellation] CancellationToken token)
    {
        await using var connection = connectionFactory.ReadOnly();

        await using var command = new NpgsqlCommand(
            """
            SELECT DISTINCT key
            FROM eventso_dlq.poison_events
            WHERE topic = @topic AND partition = @partition AND group_id = @groupId;
            """,
            connection);

        command.Parameters.Add(new NpgsqlParameter<string>("topic", topicPartition.Topic));
        command.Parameters.Add(new NpgsqlParameter<int>("partition", topicPartition.Partition.Value));
        command.Parameters.Add(new NpgsqlParameter<string>("groupId", groupId));

        await connection.OpenAsync(token);

        await using var reader = await command.ExecuteReaderAsync(token);

        while (await reader.ReadAsync(token))
            yield return reader.GetFieldValue<byte[]>(0);
    }

    public async IAsyncEnumerable<TopicPartitionOffset> GetPoisonedOffsets(
        string groupId,
        string topic,
        [EnumeratorCancellation] CancellationToken token)
    {
        await using var connection = connectionFactory.ReadOnly();

        await using var command = new NpgsqlCommand(
            """
            SELECT DISTINCT partition, "offset"
            FROM eventso_dlq.poison_events
            WHERE topic = @topic AND group_id = @groupId;
            """,
            connection);
        command.Parameters.Add(new NpgsqlParameter<string>("topic", topic));
        command.Parameters.Add(new NpgsqlParameter<string>("groupId", groupId));

        await connection.OpenAsync(token);

        await using var reader = await command.ExecuteReaderAsync(token);

        while (await reader.ReadAsync(token))
            yield return new TopicPartitionOffset(topic, new Partition(reader.GetInt32(0)), reader.GetInt64(1));
    }

    public async Task<PoisonEvent> GetEvent(
        string groupId,
        TopicPartitionOffset partitionOffset,
        CancellationToken token)
    {
        await using var connection = connectionFactory.ReadOnly();

        await using var command = new NpgsqlCommand(
            """
            SELECT 
                key,
                value,
                creation_timestamp,
                header_keys,
                header_values,
                last_failure_timestamp,
                last_failure_reason,
                total_failure_count,
                lock_timestamp,
                update_timestamp
            FROM eventso_dlq.poison_events
            WHERE topic = @topic AND partition = @partition AND "offset" = @offset AND group_id = @groupId;
            """,
            connection);
        command.Parameters.Add(new NpgsqlParameter<string>("topic", partitionOffset.Topic));
        command.Parameters.Add(new NpgsqlParameter<int>("partition", partitionOffset.Partition.Value));
        command.Parameters.Add(new NpgsqlParameter<long>("offset", partitionOffset.Offset.Value));
        command.Parameters.Add(new NpgsqlParameter<string>("groupId", groupId));

        await connection.OpenAsync(token);

        await using var reader = await command.ExecuteReaderAsync(token);

        if (!await reader.ReadAsync(token))
            throw new Exception("Poison event not found");

        var poisonEvent = new PoisonEvent(
            TopicPartitionOffset: partitionOffset,
            GroupId: groupId,
            Key: reader.GetFieldValue<byte[]>(0),
            Value: reader.GetFieldValue<byte[]>(1),
            CreationTimestamp: reader.GetDateTime(2),
            Headers: ReadHeaders(),
            LastFailureTimestamp: reader.GetDateTime(5),
            LastFailureReason: reader.GetString(6),
            TotalFailureCount: reader.GetInt32(7),
            LockTimestamp: !reader.IsDBNull(8) ? reader.GetDateTime(8) : null,
            UpdateTimestamp: reader.GetDateTime(9));

        if (await reader.ReadAsync(token))
            throw new Exception("Unexpected additional row");
        
        return poisonEvent;

        PoisonEvent.Header[] ReadHeaders()
        {
            var headerKeys = reader.GetFieldValue<string[]>(3);
            if (headerKeys.Length <= 0)
                return [];

            var headerValues = reader.GetFieldValue<byte[][]>(4);
            var headers = new PoisonEvent.Header[headerKeys.Length];
            for (var i = 0; i < headerKeys.Length; i++)
                headers[i] = new PoisonEvent.Header(headerKeys[i], headerValues[i]);
            return headers;
        }
    }

    public async Task AddEvent(
        string groupId,
        ConsumeResult<byte[], byte[]> @event,
        DateTime timestamp,
        string reason,
        CancellationToken token)
    {
        await using var connection = connectionFactory.ReadWrite();

        var headerKeys = new string[@event.Message.Headers.Count];
        var headerValues = new ReadOnlyMemory<byte>[@event.Message.Headers.Count];

        foreach (var (index, header) in @event.Message.Headers.Select((x, i) => (i, x)))
        {
            headerKeys[index] = header.Key;
            headerValues[index] = header.GetValueBytes();
        }

        await using var command = new NpgsqlCommand(
            """
            INSERT INTO eventso_dlq.poison_events(
                topic,
                partition,
                "offset",
                group_id,
                key,
                value,
                creation_timestamp,
                header_keys,
                header_values,
                last_failure_timestamp,
                last_failure_reason,
                total_failure_count,
                update_timestamp)
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
                1,
                NOW())
            ON CONFLICT ("offset", partition, topic, group_id)
            DO UPDATE
            SET
                last_failure_timestamp = excluded.last_failure_timestamp,
                last_failure_reason = excluded.last_failure_reason,
                total_failure_count = poison_events.total_failure_count + 1,
                lock_timestamp = NULL,
                update_timestamp = NOW()
            WHERE poison_events.last_failure_timestamp < excluded.last_failure_timestamp;
            """,
            connection);
        command.Parameters.Add(new NpgsqlParameter<string>("topic", @event.TopicPartitionOffset.Topic));
        command.Parameters.Add(new NpgsqlParameter<int>("partition", @event.TopicPartitionOffset.Partition.Value));
        command.Parameters.Add(new NpgsqlParameter<long>("offset", @event.TopicPartitionOffset.Offset.Value));
        command.Parameters.Add(new NpgsqlParameter<string>("groupId", groupId));
        command.Parameters.Add(new NpgsqlParameter<ReadOnlyMemory<byte>>("key", @event.Message.Key));
        command.Parameters.Add(new NpgsqlParameter<ReadOnlyMemory<byte>>("value", @event.Message.Value));
        command.Parameters.Add(new NpgsqlParameter<DateTime>("creationTimestamp", @event.Message.Timestamp.UtcDateTime));
        command.Parameters.Add(new NpgsqlParameter<string[]>("headerKeys", headerKeys));
        command.Parameters.Add(new NpgsqlParameter<ReadOnlyMemory<byte>[]>("headerValues", headerValues));
        command.Parameters.Add(new NpgsqlParameter<DateTime>("lastFailureTimestamp", timestamp));
        command.Parameters.Add(new NpgsqlParameter<string>("lastFailureReason", reason));

        await connection.OpenAsync(token);

        await command.ExecuteNonQueryAsync(token);
    }

    public async Task RemoveEvent(string groupId, TopicPartitionOffset partitionOffset, CancellationToken token)
    {
        await using var connection = connectionFactory.ReadWrite();

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
}