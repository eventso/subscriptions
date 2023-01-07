using Confluent.Kafka;
using Eventso.Subscription.Kafka.DeadLetter.Postgres;
using Eventso.Subscription.Kafka.DeadLetter.Store;
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

        await PoisonEventStore.Initialize(database.ConnectionFactory);

        await using var connection = database.ConnectionFactory.ReadWrite();

        await using var command = new NpgsqlCommand(
            $"SELECT 1 FROM eventso_dlq.poison_events;",
            connection);

        await connection.OpenAsync();
        await command.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task AddSingleToStore_EventsAdded()
    {
        await using var database = await Database.Create(); 
        var store = await PoisonEventStore.Initialize(database.ConnectionFactory);

        var timestamp = _fixture.Create<DateTime>();
        var @event = _fixture.Create<OpeningPoisonEvent>();

        await store.Add(timestamp, @event, CancellationToken.None);

        var storedEvents = await GetStoredEvents(database);
        storedEvents.Should().ContainSingle()
            .Subject
            .Should()
            .BeEquivalentTo(
                new PoisonEventRaw(
                    @event.TopicPartitionOffset.Topic,
                    @event.TopicPartitionOffset.Partition.Value,
                    @event.TopicPartitionOffset.Offset.Value,
                    @event.Key,
                    @event.Value.ToArray(),
                    @event.CreationTimestamp,
                    @event.Headers.Select(h => h.Key).ToArray(),
                    @event.Headers.Select(h => h.Data.ToArray()).ToArray(),
                    timestamp,
                    @event.FailureReason,
                    1),
                o => o.AcceptingCloseDateTimes());
    }

    [Fact]
    public async Task AddBulkToStore_EventsAdded()
    {
        await using var database = await Database.Create(); 
        var store = await PoisonEventStore.Initialize(database.ConnectionFactory);

        var timestamp = _fixture.Create<DateTime>();
        var events = _fixture.CreateMany<OpeningPoisonEvent>().ToArray();

        await store.Add(timestamp, events, CancellationToken.None);

        var storedEvents = await GetStoredEvents(database);
        events.Select(e => new PoisonEventRaw(
                e.TopicPartitionOffset.Topic,
                e.TopicPartitionOffset.Partition.Value,
                e.TopicPartitionOffset.Offset.Value,
                e.Key,
                e.Value.ToArray(),
                e.CreationTimestamp,
                e.Headers.Select(h => h.Key).ToArray(),
                e.Headers.Select(h => h.Data.ToArray()).ToArray(),
                timestamp,
                e.FailureReason,
                1))
            .Should()
            .BeEquivalentTo(storedEvents, o => o.AcceptingCloseDateTimes());
    }

    [Fact]
    public async Task AddFailureToStore_FailuresAdded()
    {
        await using var database = await Database.Create(); 
        var store = await PoisonEventStore.Initialize(database.ConnectionFactory);

        var timestamp = _fixture.Create<DateTime>();
        var events = _fixture.CreateMany<OpeningPoisonEvent>().ToArray();
        await store.Add(timestamp, events, CancellationToken.None);

        var updatedTimestamp = _fixture.Create<DateTime>();
        var updatedFailure = new OccuredFailure(
            events.OrderBy(_ => Guid.NewGuid()).First().TopicPartitionOffset,
            _fixture.Create<string>());

        await store.AddFailure(updatedTimestamp, updatedFailure, CancellationToken.None);

        var storedEvents = await GetStoredEvents(database);
        events.Select(e => new PoisonEventRaw(
                e.TopicPartitionOffset.Topic,
                e.TopicPartitionOffset.Partition.Value,
                e.TopicPartitionOffset.Offset.Value,
                e.Key,
                e.Value.ToArray(),
                e.CreationTimestamp,
                e.Headers.Select(h => h.Key).ToArray(),
                e.Headers.Select(h => h.Data.ToArray()).ToArray(),
                updatedFailure.TopicPartitionOffset == e.TopicPartitionOffset ? updatedTimestamp : timestamp,
                updatedFailure.TopicPartitionOffset == e.TopicPartitionOffset ? updatedFailure.Reason : e.FailureReason,
                updatedFailure.TopicPartitionOffset == e.TopicPartitionOffset ? 2 : 1))
            .Should()
            .BeEquivalentTo(storedEvents, o => o.AcceptingCloseDateTimes());
    }

    [Fact]
    public async Task AddFailuresToStore_FailuresAdded()
    {
        await using var database = await Database.Create(); 
        var store = await PoisonEventStore.Initialize(database.ConnectionFactory);

        var timestamp = _fixture.Create<DateTime>();
        var events = _fixture.CreateMany<OpeningPoisonEvent>().ToArray();
        await store.Add(timestamp, events, CancellationToken.None);

        var updatedTimestamp = _fixture.Create<DateTime>();
        var updatedFailures = events
            .OrderBy(_ => Guid.NewGuid())
            .Skip(1)
            .Select(u => new OccuredFailure(u.TopicPartitionOffset, _fixture.Create<string>()))
            .ToDictionary(f => f.TopicPartitionOffset);

        await store.AddFailures(updatedTimestamp, updatedFailures.Values, CancellationToken.None);

        var storedEvents = await GetStoredEvents(database);
        events.Select(e => new PoisonEventRaw(
                e.TopicPartitionOffset.Topic,
                e.TopicPartitionOffset.Partition.Value,
                e.TopicPartitionOffset.Offset.Value,
                e.Key,
                e.Value.ToArray(),
                e.CreationTimestamp,
                e.Headers.Select(h => h.Key).ToArray(),
                e.Headers.Select(h => h.Data.ToArray()).ToArray(),
                updatedFailures.ContainsKey(e.TopicPartitionOffset) ? updatedTimestamp : timestamp,
                updatedFailures.TryGetValue(e.TopicPartitionOffset, out var failure) ? failure.Reason : e.FailureReason,
                updatedFailures.ContainsKey(e.TopicPartitionOffset) ? 2 : 1))
            .Should()
            .BeEquivalentTo(storedEvents, o => o.AcceptingCloseDateTimes());
    }

    [Fact]
    public async Task RemoveSingleFromStore_EventsRemoved()
    {
        await using var database = await Database.Create(); 
        var store = await PoisonEventStore.Initialize(database.ConnectionFactory);

        var timestamp = _fixture.Create<DateTime>();
        var events = _fixture.CreateMany<OpeningPoisonEvent>(10).ToArray();
        await store.Add(timestamp, events, CancellationToken.None);

        var toRemove = events.OrderBy(_ => Guid.NewGuid()).First().TopicPartitionOffset;

        await store.Remove(toRemove, CancellationToken.None);

        var storedEvents = await GetStoredEvents(database);
        events
            .Where(e => toRemove != e.TopicPartitionOffset)
            .Select(e => new PoisonEventRaw(
                e.TopicPartitionOffset.Topic,
                e.TopicPartitionOffset.Partition.Value,
                e.TopicPartitionOffset.Offset.Value,
                e.Key,
                e.Value.ToArray(),
                e.CreationTimestamp,
                e.Headers.Select(h => h.Key).ToArray(),
                e.Headers.Select(h => h.Data.ToArray()).ToArray(),
                timestamp,
                e.FailureReason,
                1))
            .Should()
            .BeEquivalentTo(storedEvents, o => o.AcceptingCloseDateTimes());
    }

    [Fact]
    public async Task RemoveBulkFromStore_EventsRemoved()
    {
        await using var database = await Database.Create(); 
        var store = await PoisonEventStore.Initialize(database.ConnectionFactory);

        var timestamp = _fixture.Create<DateTime>();
        var events = _fixture.CreateMany<OpeningPoisonEvent>(10).ToArray();
        await store.Add(timestamp, events, CancellationToken.None);

        var toRemove = events.OrderBy(_ => Guid.NewGuid()).Take(5).Select(e => e.TopicPartitionOffset).ToArray();

        await store.Remove(toRemove, CancellationToken.None);

        var storedEvents = await GetStoredEvents(database);
        events
            .Where(e => !toRemove.Contains(e.TopicPartitionOffset))
            .Select(e => new PoisonEventRaw(
                e.TopicPartitionOffset.Topic,
                e.TopicPartitionOffset.Partition.Value,
                e.TopicPartitionOffset.Offset.Value,
                e.Key,
                e.Value.ToArray(),
                e.CreationTimestamp,
                e.Headers.Select(h => h.Key).ToArray(),
                e.Headers.Select(h => h.Data.ToArray()).ToArray(),
                timestamp,
                e.FailureReason,
                1))
            .Should()
            .BeEquivalentTo(storedEvents, o => o.AcceptingCloseDateTimes());
    }

    [Fact]
    public async Task Count_MeetsExpected()
    {
        await using var database = await Database.Create(); 
        var store = await PoisonEventStore.Initialize(database.ConnectionFactory);

        var timestamp = _fixture.Create<DateTime>();
        var expectedCount = _fixture.Create<byte>() % 10 + 1;
        var events = _fixture.CreateMany<OpeningPoisonEvent>(expectedCount * 2).ToArray();
        await store.Add(timestamp, events, CancellationToken.None);

        var toRemove = events.OrderBy(_ => Guid.NewGuid()).Take(expectedCount).Select(e => e.TopicPartitionOffset).ToArray();

        await store.Remove(toRemove, CancellationToken.None);

        var storedEvents = await GetStoredEvents(database);
        storedEvents.Should().HaveCount(expectedCount);
    }

    [Fact]
    public async Task IsKeyStored_MeetsExpected()
    {
        await using var database = await Database.Create(); 
        var store = await PoisonEventStore.Initialize(database.ConnectionFactory);

        var timestamp = _fixture.Create<DateTime>();
        var events = _fixture.CreateMany<OpeningPoisonEvent>(3).ToArray();
        await store.Add(timestamp, events, CancellationToken.None);

        foreach (var @event in events)
        {
            var isStored = await store.IsStreamStored(@event.TopicPartitionOffset.Topic, @event.Key, CancellationToken.None);
            isStored.Should().BeTrue();

            var isNotStored = await store.IsStreamStored(@event.TopicPartitionOffset.Topic, Guid.NewGuid(), CancellationToken.None);
            isNotStored.Should().BeFalse();
        }
    }

    [Fact]
    public async Task GetStoredKeys_MeetsExpected()
    {
        await using var database = await Database.Create(); 
        var store = await PoisonEventStore.Initialize(database.ConnectionFactory);

        var timestamp = _fixture.Create<DateTime>();
        var events = _fixture.CreateMany<OpeningPoisonEvent>(3).ToArray();
        await store.Add(timestamp, events, CancellationToken.None);

        var knownKeys = events.Select(e => new StreamId(e.TopicPartitionOffset.Topic, e.Key)).ToHashSet();
        var storedKeys = store.GetStoredStreams(
            knownKeys.Union(_fixture.CreateMany<StreamId>(3)).ToArray(),
            CancellationToken.None);
            
        await foreach (var storedKey in storedKeys)
            knownKeys.Remove(storedKey).Should().BeTrue();
        knownKeys.Should().BeEmpty();
    }

    [Fact]
    public async Task AcquireEventsForRetrying_MeetsExpected()
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

        await using var database = await Database.Create();
        var store = await PoisonEventStore.Initialize(
            database.ConnectionFactory,
            maxFailureCount,
            minIntervalBetweenRetries,
            maxLockHandleInterval);

        var firstKey = Guid.NewGuid();
        var secondKey = Guid.NewGuid();
        var thirdKey = Guid.NewGuid();
        var fourthKey = Guid.NewGuid();
        var fifthKey = Guid.NewGuid();
        var sixthKey = Guid.NewGuid();
        var seventhKey = Guid.NewGuid();

        var events = await Task.WhenAll(
            CreateEvent(firstKey, new TopicPartitionOffset(relevantTopic, 10, 1), canBeRetriedLastFailureTimestamp, cantBeRetriedFailureCount, null),
            CreateEvent(firstKey, new TopicPartitionOffset(relevantTopic, 10, 2), canBeRetriedLastFailureTimestamp, canBeRetriedFailureCount, null),
            CreateEvent(firstKey, new TopicPartitionOffset(relevantTopic, 10, 3), canBeRetriedLastFailureTimestamp, canBeRetriedFailureCount, null),
            CreateEvent(secondKey, new TopicPartitionOffset(relevantTopic, 20, 1), canBeRetriedLastFailureTimestamp, canBeRetriedFailureCount, null),
            CreateEvent(secondKey, new TopicPartitionOffset(relevantTopic, 20, 2), cantBeRetriedLastFailureTimestamp, canBeRetriedFailureCount, null),
            CreateEvent(secondKey, new TopicPartitionOffset(relevantTopic, 20, 3), canBeRetriedLastFailureTimestamp, cantBeRetriedFailureCount, null),
            CreateEvent(thirdKey, new TopicPartitionOffset(relevantTopic, 30, 1), canBeRetriedLastFailureTimestamp, cantBeRetriedFailureCount, null),
            CreateEvent(thirdKey, new TopicPartitionOffset(relevantTopic, 30, 2), canBeRetriedLastFailureTimestamp, canBeRetriedFailureCount, null),
            CreateEvent(thirdKey, new TopicPartitionOffset(relevantTopic, 30, 3), canBeRetriedLastFailureTimestamp, canBeRetriedFailureCount, null),
            CreateEvent(fourthKey, new TopicPartitionOffset(relevantTopic, 40, 1), canBeRetriedLastFailureTimestamp, canBeRetriedFailureCount, null),
            CreateEvent(fourthKey, new TopicPartitionOffset(relevantTopic, 40, 2), canBeRetriedLastFailureTimestamp, canBeRetriedFailureCount, null),
            CreateEvent(fourthKey, new TopicPartitionOffset(relevantTopic, 40, 3), canBeRetriedLastFailureTimestamp, canBeRetriedFailureCount, null),
            CreateEvent(fifthKey, new TopicPartitionOffset(irrelevantTopic, 50, 1), canBeRetriedLastFailureTimestamp, canBeRetriedFailureCount, null),
            CreateEvent(fifthKey, new TopicPartitionOffset(irrelevantTopic, 50, 2), canBeRetriedLastFailureTimestamp, canBeRetriedFailureCount, null),
            CreateEvent(fifthKey, new TopicPartitionOffset(irrelevantTopic, 50, 3), canBeRetriedLastFailureTimestamp, canBeRetriedFailureCount, null),
            CreateEvent(sixthKey, new TopicPartitionOffset(relevantTopic, 60, 1), canBeRetriedLastFailureTimestamp, canBeRetriedFailureCount, canBeRetriedLockHandleTimestamp),
            CreateEvent(sixthKey, new TopicPartitionOffset(relevantTopic, 60, 2), canBeRetriedLastFailureTimestamp, canBeRetriedFailureCount, null),
            CreateEvent(sixthKey, new TopicPartitionOffset(relevantTopic, 60, 3), canBeRetriedLastFailureTimestamp, canBeRetriedFailureCount, null),
            CreateEvent(seventhKey, new TopicPartitionOffset(relevantTopic, 70, 1), canBeRetriedLastFailureTimestamp, canBeRetriedFailureCount, cantBeRetriedLockHandleTimestamp),
            CreateEvent(seventhKey, new TopicPartitionOffset(relevantTopic, 70, 2), canBeRetriedLastFailureTimestamp, canBeRetriedFailureCount, null),
            CreateEvent(seventhKey, new TopicPartitionOffset(relevantTopic, 70, 3), canBeRetriedLastFailureTimestamp, canBeRetriedFailureCount, null));

        var eventsForRetrying = new List<StoredPoisonEvent>();
        await foreach (var @event in store.AcquireEventsForRetrying(relevantTopic, CancellationToken.None))
            eventsForRetrying.Add(@event);

        eventsForRetrying
            .Should()
            .BeEquivalentTo(
                events
                    .Where(e => (e.Key == secondKey || e.Key == fourthKey || e.Key == sixthKey)
                                && e.TopicPartitionOffset.Offset.Value == 1)
                    .Select(e => new StoredPoisonEvent(
                        e.TopicPartitionOffset.Partition,
                        e.TopicPartitionOffset.Offset,
                        e.Key,
                        e.Value,
                        e.CreationTimestamp,
                        e.Headers,
                        canBeRetriedLastFailureTimestamp,
                        e.FailureReason,
                        canBeRetriedFailureCount)),
                o => o.AcceptingCloseDateTimes().ComparingByteReadOnlyMemoryAsArrays());

        async Task<OpeningPoisonEvent> CreateEvent(
            Guid key,
            TopicPartitionOffset topicPartitionOffset,
            DateTime lastFailureTimestamp,
            int totalFailureCount,
            DateTime? lastLockTimestamp)
        {
            var @event = new OpeningPoisonEvent(
                topicPartitionOffset,
                key,
                _fixture.CreateMany<byte>().ToArray(),
                _fixture.Create<DateTime>(),
                Array.Empty<EventHeader>(),
                _fixture.Create<string>());
            await store.Add(lastFailureTimestamp, new [] { @event }, CancellationToken.None);

            await using var connection = database.ConnectionFactory.ReadWrite();

            await using var command = new NpgsqlCommand(
                @"
UPDATE eventso_dlq.poison_events pe
SET
    total_failure_count = @totalFailureCount,
    lock_timestamp = @lastLockTimestamp
WHERE pe.topic = @topic AND pe.partition = @partition AND pe.""offset"" = @offset;",
                connection)
            {
                Parameters =
                {
                    new NpgsqlParameter<string>("topic", topicPartitionOffset.Topic),
                    new NpgsqlParameter<int>("partition", topicPartitionOffset.Partition.Value),
                    new NpgsqlParameter<long>("offset", topicPartitionOffset.Offset.Value),
                    new NpgsqlParameter<int>("totalFailureCount", totalFailureCount),
                    new NpgsqlParameter("lastLockTimestamp", NpgsqlDbType.Timestamp) { NpgsqlValue = lastLockTimestamp != null ? lastLockTimestamp.Value : DBNull.Value }
                }
            };

            await connection.OpenAsync();

            await command.ExecuteNonQueryAsync();

            return @event;
        }
    }

    private static async Task<IReadOnlyCollection<PoisonEventRaw>> GetStoredEvents(Database database)
    {
        await using var connection = database.ConnectionFactory.ReadWrite();

        await using var command = new NpgsqlCommand($"SELECT * FROM eventso_dlq.poison_events;", connection);
        await connection.OpenAsync();

        var storedEvents = new List<PoisonEventRaw>();
        var reader = await command.ExecuteReaderAsync(CancellationToken.None);
        while (await reader.ReadAsync(CancellationToken.None))
        {
            storedEvents.Add(new PoisonEventRaw(
                reader.GetFieldValue<string>(0),
                reader.GetFieldValue<int>(1),
                reader.GetFieldValue<long>(2),
                reader.GetGuid(3),
                reader.GetFieldValue<byte[]>(4),
                reader.GetDateTime(5),
                reader.GetFieldValue<string[]>(6),
                reader.GetFieldValue<byte[][]>(7),
                reader.GetDateTime(8),
                reader.GetString(9),
                reader.GetInt32(10)
            ));
        }

        return storedEvents;
    }

    private sealed record PoisonEventRaw(
        string topic,
        int partition,
        long offset,
        Guid key,
        byte[] value,
        DateTime creation_timestamp,
        string[] header_keys,
        byte[][] header_values,
        DateTime last_failure_timestamp,
        string last_failure_reason,
        int total_failure_count
    );

    // to be continued

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
                
            await using var command = new NpgsqlCommand($@"
                REVOKE CONNECT ON DATABASE {_databaseName} FROM public;
                SELECT pid, pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{_databaseName}';
                DROP DATABASE {_databaseName};", connection);
                
            await connection.OpenAsync();
            await command.ExecuteNonQueryAsync();
        }

        private static string CreateCommonConnectionString()
            => string.Format(ConnectionStringFormat, "postgres");
    }
}