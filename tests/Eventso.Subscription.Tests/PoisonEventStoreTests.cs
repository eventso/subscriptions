using Confluent.Kafka;
using Eventso.Subscription.Kafka;
using Eventso.Subscription.Kafka.DeadLetter;
using Eventso.Subscription.Kafka.DeadLetter.Postgres;
using Npgsql;
using NpgsqlTypes;

namespace Eventso.Subscription.Tests;

public class PoisonEventStoreTests
{
    // comment FactAttribute line to enable tests in class
    // xUnit doesn't have "skip whole class" functionality out of the box
    // this test class requires PostgreSQL database so it is local only for now
    private class FactAttribute : Attribute { }

    private readonly IFixture _fixture = new Fixture();

    [Fact]
    public async Task InitializingStore_StoreInitialized()
    {
        await using var database = await Database.Create(); 

        await PoisonEventSchemaInitializer.Initialize(database.ConnectionFactory, default);

        await using var connection = database.ConnectionFactory.ReadWrite();

        await using var command = new NpgsqlCommand(
            $"SELECT 1 FROM eventso_dlq.poison_events;",
            connection);

        await connection.OpenAsync();
        await command.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task AddFirstTime_EventsAdded()
    {
        await using var database = await Database.Create(); 
        await PoisonEventSchemaInitializer.Initialize(database.ConnectionFactory, default);
        var store = new PoisonEventStore(database.ConnectionFactory);

        var groupId = _fixture.Create<string>();
        var timestamp = _fixture.Create<DateTime>();
        var reason = _fixture.Create<string>();
        var @event = _fixture.Create<PoisonEvent>();

        await store.AddEvent(groupId, @event, timestamp, reason, CancellationToken.None);

        var storedEvents = await GetStoredEvents(database);
        storedEvents.Should().ContainSingle()
            .Subject
            .Should()
            .BeEquivalentTo(
                new PoisonEventRaw(
                    groupId,
                    @event.TopicPartitionOffset.Topic,
                    @event.TopicPartitionOffset.Partition.Value,
                    @event.TopicPartitionOffset.Offset.Value,
                    @event.Key.ToArray(),
                    @event.Value.ToArray(),
                    @event.CreationTimestamp,
                    @event.Headers.Select(h => h.Key).ToArray(),
                    @event.Headers.Select(h => h.Data.ToArray()).ToArray(),
                    timestamp,
                    reason,
                    @event.FailureCount),
                o => o.AcceptingCloseDateTimes());
    }

    [Fact]
    public async Task AddSecondTime_EventsUpdated()
    {
        await using var database = await Database.Create(); 
        await PoisonEventSchemaInitializer.Initialize(database.ConnectionFactory, default);
        var store = new PoisonEventStore(database.ConnectionFactory);

        var groupId = _fixture.Create<string>();
        var timestamp = _fixture.Create<DateTime>();
        var reason = _fixture.Create<string>();
        var @event = _fixture.Create<PoisonEvent>();

        await store.AddEvent(groupId, @event, timestamp, reason, CancellationToken.None);
        await store.AddEvent(
            groupId,
            @event with { FailureCount = @event.FailureCount + 1 },
            timestamp, reason,
            CancellationToken.None);

        var storedEvents = await GetStoredEvents(database);
        storedEvents.Should().ContainSingle()
            .Subject
            .Should()
            .BeEquivalentTo(
                new PoisonEventRaw(
                    groupId,
                    @event.TopicPartitionOffset.Topic,
                    @event.TopicPartitionOffset.Partition.Value,
                    @event.TopicPartitionOffset.Offset.Value,
                    @event.Key.ToArray(),
                    @event.Value.ToArray(),
                    @event.CreationTimestamp,
                    @event.Headers.Select(h => h.Key).ToArray(),
                    @event.Headers.Select(h => h.Data.ToArray()).ToArray(),
                    timestamp,
                    reason,
                    @event.FailureCount + 1),
                o => o.AcceptingCloseDateTimes());
    }

    [Fact]
    public async Task AddSame_NoChanges()
    {
        await using var database = await Database.Create(); 
        await PoisonEventSchemaInitializer.Initialize(database.ConnectionFactory, default);
        var store = new PoisonEventStore(database.ConnectionFactory);

        var groupId = _fixture.Create<string>();
        var timestamp = _fixture.Create<DateTime>();
        var reason = _fixture.Create<string>();
        var @event = _fixture.Create<PoisonEvent>();

        await store.AddEvent(groupId, @event, timestamp, reason, CancellationToken.None);
        await store.AddEvent(groupId, @event, timestamp, reason, CancellationToken.None);

        var storedEvents = await GetStoredEvents(database);
        storedEvents.Should().ContainSingle()
            .Subject
            .Should()
            .BeEquivalentTo(
                new PoisonEventRaw(
                    groupId,
                    @event.TopicPartitionOffset.Topic,
                    @event.TopicPartitionOffset.Partition.Value,
                    @event.TopicPartitionOffset.Offset.Value,
                    @event.Key.ToArray(),
                    @event.Value.ToArray(),
                    @event.CreationTimestamp,
                    @event.Headers.Select(h => h.Key).ToArray(),
                    @event.Headers.Select(h => h.Data.ToArray()).ToArray(),
                    timestamp,
                    reason,
                    @event.FailureCount),
                o => o.AcceptingCloseDateTimes());
    }

    [Fact]
    public async Task RemoveSingleFromStore_EventsRemoved()
    {
        await using var database = await Database.Create(); 
        await PoisonEventSchemaInitializer.Initialize(database.ConnectionFactory, default);
        var store = new PoisonEventStore(database.ConnectionFactory);

        var groupId = _fixture.Create<string>();
        var timestamp = _fixture.Create<DateTime>();
        var reason = _fixture.Create<string>();
        var events = _fixture.CreateMany<PoisonEvent>(10).ToArray();
        foreach (var @event in events)
            await store.AddEvent(groupId, @event, timestamp, reason, CancellationToken.None);

        var toRemove = events.OrderBy(_ => Guid.NewGuid()).First().TopicPartitionOffset;

        await store.RemoveEvent(groupId, toRemove, CancellationToken.None);

        var storedEvents = await GetStoredEvents(database);
        events
            .Where(e => toRemove != e.TopicPartitionOffset)
            .Select(e => new PoisonEventRaw(
                groupId,
                e.TopicPartitionOffset.Topic,
                e.TopicPartitionOffset.Partition.Value,
                e.TopicPartitionOffset.Offset.Value,
                e.Key.ToArray(),
                e.Value.ToArray(),
                e.CreationTimestamp,
                e.Headers.Select(h => h.Key).ToArray(),
                e.Headers.Select(h => h.Data.ToArray()).ToArray(),
                timestamp,
                reason,
                e.FailureCount))
            .Should()
            .BeEquivalentTo(storedEvents, o => o.AcceptingCloseDateTimes());
    }

    [Fact]
    public async Task Count_MeetsExpected()
    {
        await using var database = await Database.Create(); 
        await PoisonEventSchemaInitializer.Initialize(database.ConnectionFactory, default);
        var store = new PoisonEventStore(database.ConnectionFactory);

        var groupId = _fixture.Create<string>();
        var timestamp = _fixture.Create<DateTime>();
        var reason = _fixture.Create<string>();
        var expectedCount = _fixture.Create<byte>() % 10 + 1;
        var events = _fixture.CreateMany<PoisonEvent>(expectedCount * 2).ToArray();
        foreach (var @event in events)
            await store.AddEvent(groupId, @event, timestamp, reason, CancellationToken.None);

        var toRemove = events.OrderBy(_ => Guid.NewGuid()).Take(expectedCount).Select(e => e.TopicPartitionOffset).ToArray();

        foreach (var topicPartitionOffset in toRemove)
        {
            await store.RemoveEvent(groupId, topicPartitionOffset, CancellationToken.None);
        }

        var storedEvents = await GetStoredEvents(database);
        storedEvents.Should().HaveCount(expectedCount);
    }

    [Fact]
    public async Task IsPoisonedKey_MeetsExpected()
    {
        await using var database = await Database.Create(); 
        await PoisonEventSchemaInitializer.Initialize(database.ConnectionFactory, default);
        var store = new PoisonEventStore(database.ConnectionFactory);

        var groupId = _fixture.Create<string>();
        var timestamp = _fixture.Create<DateTime>();
        var reason = _fixture.Create<string>();
        var events = _fixture.CreateMany<PoisonEvent>(3).ToArray();
        foreach (var @event in events)
            await store.AddEvent(groupId, @event, timestamp, reason, CancellationToken.None);

        foreach (var @event in events)
        {
            var isStored = await store.IsKeyPoisoned(groupId, @event.TopicPartitionOffset.Topic, @event.Key.ToArray(), CancellationToken.None);
            isStored.Should().BeTrue();

            var isNotStored = await store.IsKeyPoisoned(groupId, @event.TopicPartitionOffset.Topic, _fixture.Create<byte[]>(), CancellationToken.None);
            isNotStored.Should().BeFalse();
        }
    }

    [Fact]
    public async Task GetPoisonedKeys_MeetsExpected()
    {
        await using var database = await Database.Create(); 
        await PoisonEventSchemaInitializer.Initialize(database.ConnectionFactory, default);
        var store = new PoisonEventStore(database.ConnectionFactory);

        var groupId = _fixture.Create<string>();
        var timestamp = _fixture.Create<DateTime>();
        var reason = _fixture.Create<string>();
        var events = _fixture.CreateMany<PoisonEvent>(10).ToArray();
        foreach (var @event in events)
            await store.AddEvent(groupId, @event, timestamp, reason, CancellationToken.None);

        foreach (var topicPartitionEvents in events.GroupBy(e => e.TopicPartitionOffset.TopicPartition))
        {
            var expectedKeys = topicPartitionEvents.Select(e => GetKey(e.Key.ToArray())).ToArray();
            await foreach (var keyRaw in store.GetPoisonedKeys(groupId, topicPartitionEvents.Key,
                               CancellationToken.None))
                expectedKeys.Should().Contain(GetKey(keyRaw.ToArray()));
        }
    }

    [Fact]
    public async Task GetEventForRetrying_MeetsExpected()
    {
        const int maxFailureCount = 10, canBeRetriedFailureCount = 5, cantBeRetriedFailureCount = 15;
        var minIntervalBetweenRetries = TimeSpan.FromMinutes(100);
        DateTime
            canBeRetriedLastFailureTimestamp = DateTime.UtcNow.AddMinutes(-150),
            cantBeRetriedLastFailureTimestamp = DateTime.UtcNow.AddMinutes(-50);
        var maxLockHandleInterval = TimeSpan.FromMinutes(20);
        DateTime?
            canBeRetriedLockHandleTimestamp = DateTime.UtcNow.AddMinutes(-30),
            cantBeRetriedLockHandleTimestamp = DateTime.UtcNow.AddMinutes(-10);
            
        const string relevantTopic = "topic1", irrelevantTopic = "topic2";
        const string relevantGroupId = "group1", irrelevantGroupId = "group2";

        await using var database = await Database.Create();
        await PoisonEventSchemaInitializer.Initialize(database.ConnectionFactory, default);
        var store = new PoisonEventStore(database.ConnectionFactory);
        var scheduler = new PoisonEventRetryingScheduler(
            database.ConnectionFactory,
            maxFailureCount,
            minIntervalBetweenRetries,
            maxLockHandleInterval);

        var reason = _fixture.Create<string>();

        var firstKey = _fixture.CreateMany<byte>(16).ToArray();
        var secondKey = _fixture.CreateMany<byte>(16).ToArray();
        var thirdKey = _fixture.CreateMany<byte>(16).ToArray();
        var fourthKey = _fixture.CreateMany<byte>(16).ToArray();
        var fifthKey = _fixture.CreateMany<byte>(16).ToArray();
        var sixthKey = _fixture.CreateMany<byte>(16).ToArray();
        var seventhKey = _fixture.CreateMany<byte>(16).ToArray();
        var eighthKey = _fixture.CreateMany<byte>(16).ToArray();

        var events = await Task.WhenAll(
            CreateEvent(relevantGroupId, firstKey, new TopicPartitionOffset(relevantTopic, 10, 1), canBeRetriedLastFailureTimestamp, reason, cantBeRetriedFailureCount, null),
            CreateEvent(relevantGroupId, firstKey, new TopicPartitionOffset(relevantTopic, 10, 2), canBeRetriedLastFailureTimestamp, reason, canBeRetriedFailureCount, null),
            CreateEvent(relevantGroupId, firstKey, new TopicPartitionOffset(relevantTopic, 10, 3), canBeRetriedLastFailureTimestamp, reason, canBeRetriedFailureCount, null),
            CreateEvent(relevantGroupId, secondKey, new TopicPartitionOffset(relevantTopic, 20, 1), canBeRetriedLastFailureTimestamp, reason, canBeRetriedFailureCount, null),
            CreateEvent(relevantGroupId, secondKey, new TopicPartitionOffset(relevantTopic, 20, 2), cantBeRetriedLastFailureTimestamp, reason, canBeRetriedFailureCount, null),
            CreateEvent(relevantGroupId, secondKey, new TopicPartitionOffset(relevantTopic, 20, 3), canBeRetriedLastFailureTimestamp, reason, cantBeRetriedFailureCount, null),
            CreateEvent(relevantGroupId, thirdKey, new TopicPartitionOffset(relevantTopic, 30, 1), canBeRetriedLastFailureTimestamp, reason, cantBeRetriedFailureCount, null),
            CreateEvent(relevantGroupId, thirdKey, new TopicPartitionOffset(relevantTopic, 30, 2), canBeRetriedLastFailureTimestamp, reason, canBeRetriedFailureCount, null),
            CreateEvent(relevantGroupId, thirdKey, new TopicPartitionOffset(relevantTopic, 30, 3), canBeRetriedLastFailureTimestamp, reason, canBeRetriedFailureCount, null),
            CreateEvent(relevantGroupId, fourthKey, new TopicPartitionOffset(relevantTopic, 40, 1), canBeRetriedLastFailureTimestamp, reason, canBeRetriedFailureCount, null),
            CreateEvent(relevantGroupId, fourthKey, new TopicPartitionOffset(relevantTopic, 40, 2), canBeRetriedLastFailureTimestamp, reason, canBeRetriedFailureCount, null),
            CreateEvent(relevantGroupId, fourthKey, new TopicPartitionOffset(relevantTopic, 40, 3), canBeRetriedLastFailureTimestamp, reason, canBeRetriedFailureCount, null),
            CreateEvent(relevantGroupId, fifthKey, new TopicPartitionOffset(irrelevantTopic, 50, 1), canBeRetriedLastFailureTimestamp, reason, canBeRetriedFailureCount, null),
            CreateEvent(relevantGroupId, fifthKey, new TopicPartitionOffset(irrelevantTopic, 50, 2), canBeRetriedLastFailureTimestamp, reason, canBeRetriedFailureCount, null),
            CreateEvent(relevantGroupId, fifthKey, new TopicPartitionOffset(irrelevantTopic, 50, 3), canBeRetriedLastFailureTimestamp, reason, canBeRetriedFailureCount, null),
            CreateEvent(relevantGroupId, sixthKey, new TopicPartitionOffset(relevantTopic, 20, 4), canBeRetriedLastFailureTimestamp, reason, canBeRetriedFailureCount, canBeRetriedLockHandleTimestamp),
            CreateEvent(relevantGroupId, sixthKey, new TopicPartitionOffset(relevantTopic, 20, 5), canBeRetriedLastFailureTimestamp, reason, canBeRetriedFailureCount, null),
            CreateEvent(relevantGroupId, sixthKey, new TopicPartitionOffset(relevantTopic, 20, 6), canBeRetriedLastFailureTimestamp, reason, canBeRetriedFailureCount, null),
            CreateEvent(relevantGroupId, seventhKey, new TopicPartitionOffset(relevantTopic, 70, 1), canBeRetriedLastFailureTimestamp, reason, canBeRetriedFailureCount, cantBeRetriedLockHandleTimestamp),
            CreateEvent(relevantGroupId, seventhKey, new TopicPartitionOffset(relevantTopic, 70, 2), canBeRetriedLastFailureTimestamp, reason, canBeRetriedFailureCount, null),
            CreateEvent(relevantGroupId, seventhKey, new TopicPartitionOffset(relevantTopic, 70, 3), canBeRetriedLastFailureTimestamp, reason, canBeRetriedFailureCount, null),
            CreateEvent(irrelevantGroupId, eighthKey, new TopicPartitionOffset(relevantTopic, 80, 1), canBeRetriedLastFailureTimestamp, reason, canBeRetriedFailureCount, null),
            CreateEvent(irrelevantGroupId, eighthKey, new TopicPartitionOffset(relevantTopic, 80, 2), canBeRetriedLastFailureTimestamp, reason, canBeRetriedFailureCount, null),
            CreateEvent(irrelevantGroupId, eighthKey, new TopicPartitionOffset(relevantTopic, 80, 3), canBeRetriedLastFailureTimestamp, reason, canBeRetriedFailureCount, null));

        
        var eventsForRetrying = new List<PoisonEvent>();
        while (true)
        {
            var @event = await scheduler.GetEventForRetrying(
                relevantGroupId,
                new TopicPartition(relevantTopic, 20),
                CancellationToken.None);
            
            await using var connection = database.ConnectionFactory.ReadWrite();

            await using var command = new NpgsqlCommand($"SELECT * FROM eventso_dlq.poison_events;", connection);
            await connection.OpenAsync();
            
            if (@event == null)
                break;
            
            eventsForRetrying.Add(@event);
        }

        eventsForRetrying
            .Should()
            .BeEquivalentTo(
                events
                    .Where(e => (e.Key.ToArray().SequenceEqual(secondKey) && e.TopicPartitionOffset.Offset == 1)
                                || (e.Key.ToArray().SequenceEqual(sixthKey) && e.TopicPartitionOffset.Offset == 4)),
                o => o.AcceptingCloseDateTimes().ComparingByteReadOnlyMemoryAsArrays());

        async Task<PoisonEvent> CreateEvent(
            string groupId,
            byte[] key,
            TopicPartitionOffset topicPartitionOffset,
            DateTime lastFailureTimestamp,
            string lastFailureReason,
            int totalFailureCount,
            DateTime? lastLockTimestamp)
        {
            var @event = new PoisonEvent(
                topicPartitionOffset,
                key,
                _fixture.CreateMany<byte>().ToArray(),
                _fixture.Create<DateTime>(),
                Array.Empty<PoisonEvent.Header>(),
                totalFailureCount);
            await store.AddEvent(groupId, @event, lastFailureTimestamp, lastFailureReason, CancellationToken.None);

            await using var connection = database.ConnectionFactory.ReadWrite();

            await using var command = new NpgsqlCommand(
                @"
UPDATE eventso_dlq.poison_events pe
SET
    lock_timestamp = @lastLockTimestamp
WHERE group_id = @groupId AND pe.topic = @topic AND pe.partition = @partition AND pe.""offset"" = @offset;",
                connection)
            {
                Parameters =
                {
                    new NpgsqlParameter<string>("groupId", groupId),
                    new NpgsqlParameter<string>("topic", topicPartitionOffset.Topic),
                    new NpgsqlParameter<int>("partition", topicPartitionOffset.Partition.Value),
                    new NpgsqlParameter<long>("offset", topicPartitionOffset.Offset.Value),
                    new NpgsqlParameter("lastLockTimestamp", NpgsqlDbType.Timestamp)
                    {
                        NpgsqlValue = lastLockTimestamp != null
                            ? DateTime.SpecifyKind(lastLockTimestamp.Value, DateTimeKind.Unspecified)
                            : DBNull.Value
                    }
                }
            };

            await connection.OpenAsync();

            await command.ExecuteNonQueryAsync();

            return @event;
        }
    }
    private sealed record PoisonEventRaw2(
        string groupId,
        string topic,
        int partition,
        long offset,
        byte[] key,
        byte[] value,
        DateTime creation_timestamp,
        string[] header_keys,
        byte[][] header_values,
        DateTime last_failure_timestamp,
        string last_failure_reason,
        int total_failure_count,
        DateTime? lock_timestamp
    );

    private static async Task<IReadOnlyCollection<PoisonEventRaw>> GetStoredEvents(Database database)
    {
        await using var connection = database.ConnectionFactory.ReadWrite();

        await using var command = new NpgsqlCommand($"SELECT * FROM eventso_dlq.poison_events;", connection);
        await connection.OpenAsync();

        var storedEvents = new List<PoisonEventRaw>();
        await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
        while (await reader.ReadAsync(CancellationToken.None))
        {
            storedEvents.Add(new PoisonEventRaw(
                reader.GetFieldValue<string>(0),
                reader.GetFieldValue<string>(1),
                reader.GetFieldValue<int>(2),
                reader.GetFieldValue<long>(3),
                reader.GetFieldValue<byte[]>(4),
                reader.GetFieldValue<byte[]>(5),
                reader.GetDateTime(6),
                reader.GetFieldValue<string[]>(7),
                reader.GetFieldValue<byte[][]>(8),
                reader.GetDateTime(9),
                reader.GetString(10),
                reader.GetInt32(11)
            ));
        }

        return storedEvents;
    }

    private static Guid GetKey(byte[] key)
        => KeyGuidDeserializer.Instance.Deserialize(key, false, SerializationContext.Empty);

    private sealed record PoisonEventRaw(
        string groupId,
        string topic,
        int partition,
        long offset,
        byte[] key,
        byte[] value,
        DateTime creation_timestamp,
        string[] header_keys,
        byte[][] header_values,
        DateTime last_failure_timestamp,
        string last_failure_reason,
        int total_failure_count
    );

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
            await connection.OpenAsync();

            await using var cleanupCommand = new NpgsqlCommand($@"
                REVOKE CONNECT ON DATABASE {_databaseName} FROM public;
                SELECT pid, pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{_databaseName}';",
                connection);
            await cleanupCommand.ExecuteNonQueryAsync();

            await using var dropCommand = new NpgsqlCommand($"DROP DATABASE {_databaseName};", connection);
            await dropCommand.ExecuteNonQueryAsync();
        }

        private static string CreateCommonConnectionString()
            => string.Format(ConnectionStringFormat, "postgres");
    }
}