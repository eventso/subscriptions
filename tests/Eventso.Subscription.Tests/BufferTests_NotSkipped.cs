using System.Threading.Channels;

namespace Eventso.Subscription.Tests;

public sealed class BufferTests_NotSkipped : IDisposable
{
    private readonly Fixture _fixture = new();

    private readonly Channel<Buffer<RedMessage>.Batch> _bufferChannel =
        Channel.CreateUnbounded<Buffer<RedMessage>.Batch>();

    private readonly SemaphoreSlim _semaphore = new(0);
    private readonly List<Buffer<RedMessage>.Batch> _processed = new();

    public void Dispose()
    {
        _semaphore.Dispose();
    }

    [Theory]
    [InlineData(4, 12)]
    [InlineData(2, 5)]
    [InlineData(10, 5)]
    public async Task AddingItem_CorrectBatching(
        int maxBatchSize,
        int eventsCount)
    {
        using var buffer = new Buffer<RedMessage>(
            maxBatchSize,
            Timeout.InfiniteTimeSpan,
            _bufferChannel,
            maxBatchSize * 3,
            CancellationToken.None);

        var events = _fixture.CreateMany<RedMessage>(eventsCount).ToArray();

        foreach (var @event in events)
            await buffer.Add(@event, skipped: false, CancellationToken.None);

        await buffer.Complete();

        _bufferChannel.Writer.Complete();
        var batches = new List<Buffer<RedMessage>.Batch>();

        while (_bufferChannel.Reader.TryRead(out var batch))
            batches.Add(batch);

        batches.Should().HaveCount((int)Math.Ceiling(new decimal(eventsCount) / maxBatchSize));
        batches.SelectMany(x => x.Events.Segment).Select(x => x.Event)
            .Should()
            .BeEquivalentTo(events, c => c.WithStrictOrdering());
    }

    [Fact]
    public async Task AddingItem_BatchesProcessedInStreamedWay()
    {
        const int maxBatchSize = 3;
        using var buffer = new Buffer<RedMessage>(
            maxBatchSize,
            Timeout.InfiniteTimeSpan,
            _bufferChannel,
            maxBatchSize * 3,
            CancellationToken.None);

        var events = _fixture.CreateMany<RedMessage>(maxBatchSize * 3).ToArray();

        for (var iteration = 0; iteration < 2; iteration++)
        {
            for (var i = 0; i < maxBatchSize; i++)
                await buffer.Add(events[i + (iteration * maxBatchSize)], skipped: false, CancellationToken.None);

            await Task.Delay(100);

            var batches = new List<Buffer<RedMessage>.Batch>();
            while (_bufferChannel.Reader.TryRead(out var bufferBatch))
                batches.Add(bufferBatch);

            using var batch = batches.Should().ContainSingle().Subject.Events;
            batch.Segment.Should().HaveCount(maxBatchSize);
            batch.Segment.Select(x => x.Event)
                .Should()
                .BeEquivalentTo(
                    events.Skip(iteration * maxBatchSize).Take(maxBatchSize),
                    c => c.WithStrictOrdering());
        }
    }

    [Fact]
    public async Task OnTimeout_BatchCreated()
    {
        const int maxBatchSize = 50;
        const int batchTimeoutMs = 1000;
        const int singleEventDelayMs = 200;

        using var buffer = new Buffer<RedMessage>(
            maxBatchSize,
            TimeSpan.FromMilliseconds(batchTimeoutMs),
            _bufferChannel,
            maxBatchSize * 3,
            CancellationToken.None);

        var events = _fixture.CreateMany<RedMessage>(maxBatchSize).ToArray();

        foreach (var @event in events)
        {
            await buffer.Add(@event, skipped: false, CancellationToken.None);
            await Task.Delay(singleEventDelayMs);
        }

        await buffer.Complete();

        var batches = new List<Buffer<RedMessage>.Batch>();
        while (_bufferChannel.Reader.TryRead(out var bufferBatch))
            batches.Add(bufferBatch);


        batches.Should().HaveCount(events.Length / (batchTimeoutMs / singleEventDelayMs));
        batches.SelectMany(x => x.Events.Segment).Select(x => x.Event)
            .Should()
            .BeEquivalentTo(events, c => c.WithStrictOrdering());
    }

    [Fact]
    public async Task AddingManyItemsWithTimeout_NoChanceForTimeout()
    {
        const int maxBatchSize = 5;
        const int eventsCount = 100;
        const int batchTimeoutMs = 200;
        const int singleEventDelayMs = 10;

        using var buffer = new Buffer<RedMessage>(
            maxBatchSize,
            TimeSpan.FromMilliseconds(batchTimeoutMs),
            _bufferChannel,
            maxBatchSize * 3,
            CancellationToken.None);

        var events = _fixture.CreateMany<RedMessage>(eventsCount).ToArray();

        foreach (var @event in events)
        {
            await buffer.Add(@event, skipped: false, CancellationToken.None);
            await Task.Delay(singleEventDelayMs);
        }

        await buffer.Complete();

        var batches = new List<Buffer<RedMessage>.Batch>();
        while (_bufferChannel.Reader.TryRead(out var bufferBatch))
            batches.Add(bufferBatch);


        batches.Should().HaveCount(events.Length / maxBatchSize);
        batches.Should().OnlyContain(list => list.Events.Count == maxBatchSize);
        batches.SelectMany(x => x.Events.Segment).Select(x => x.Event)
            .Should()
            .BeEquivalentTo(events, c => c.WithStrictOrdering());
    }

    [Fact]
    public async Task Completing_EventsFlushed()
    {
        const int maxBatchSize = 5000;
        const int eventsCount = 10;

        using var buffer = new Buffer<RedMessage>(
            maxBatchSize,
            Timeout.InfiniteTimeSpan,
            _bufferChannel,
            maxBatchSize * 3,
            CancellationToken.None);

        var events = _fixture.CreateMany<RedMessage>(eventsCount).ToArray();

        foreach (var @event in events)
            await buffer.Add(@event, skipped: false, CancellationToken.None);

        await buffer.Complete();

        var batches = new List<Buffer<RedMessage>.Batch>();
        while (_bufferChannel.Reader.TryRead(out var bufferBatch))
            batches.Add(bufferBatch);

        var batch = batches.Should().ContainSingle().Subject;
        batch.Events.Segment.Select(x => x.Event)
            .Should()
            .BeEquivalentTo(events, c => c.WithStrictOrdering());
    }

    [Fact]
    public async Task AddingItemToCompleted_Throws()
    {
        const int maxBatchSize = 10;
        const int eventsCount = 10;

        using var buffer = new Buffer<RedMessage>(
            maxBatchSize,
            Timeout.InfiniteTimeSpan,
            _bufferChannel,
            maxBatchSize * 3,
            CancellationToken.None);

        var events = _fixture.CreateMany<RedMessage>(eventsCount).ToArray();

        foreach (var @event in events)
            await buffer.Add(@event, skipped: false, CancellationToken.None);

        await buffer.Complete();

        var act = () => buffer.Add(
            _fixture.Create<RedMessage>(), false, CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>();
    }

    [Fact]
    public async Task AddingItemToBlockedTarget_ItemAdded()
    {
        const int maxBatchSize = 10;
        const int eventsCount = 30;
        var bufferChannel =
            Channel.CreateBounded<Buffer<RedMessage>.Batch>(1);

        using var buffer = new Buffer<RedMessage>(
            maxBatchSize,
            Timeout.InfiniteTimeSpan,
            bufferChannel,
            maxBatchSize * 3,
            CancellationToken.None);

        _ = RunSemaphoreRead(bufferChannel);

        var events = _fixture.CreateMany<RedMessage>(eventsCount).ToArray();

        foreach (var @event in events)
            await buffer.Add(@event, skipped: false, CancellationToken.None);

        var completeTask = buffer.Complete();

        await Task.Delay(100);

        completeTask.IsCompleted.Should().BeFalse();
        _processed.Count.Should().Be(1);

        _semaphore.Release();

        await completeTask;
        bufferChannel.Writer.Complete();
        await bufferChannel.Reader.Completion;

        _processed.Should().HaveCount(events.Length / maxBatchSize);
        _processed.SelectMany(x => x.Events.Segment).Select(x => x.Event)
            .Should()
            .BeEquivalentTo(events, c => c.WithStrictOrdering());
    }

    [Fact]
    public async Task AddingItemOverBufferCapacityToBlockedTarget_AddingBlocked()
    {
        const int maxBatchSize = 10;
        const int eventsCount = 31;

        var bufferChannel = Channel.CreateBounded<Buffer<RedMessage>.Batch>(1);

        using var buffer = new Buffer<RedMessage>(
            maxBatchSize,
            Timeout.InfiniteTimeSpan,
            bufferChannel,
            maxBatchSize * 3,
            CancellationToken.None);

        var batchReadTask = RunSemaphoreRead(bufferChannel);

        var events = _fixture.CreateMany<RedMessage>(eventsCount).ToArray();

        foreach (var @event in events)
            await buffer.Add(@event, skipped: false, CancellationToken.None);

        var overBufferEvent = _fixture.Create<RedMessage>();
        var addingTask = buffer.Add(overBufferEvent, skipped: false, CancellationToken.None);

        await Task.Delay(100);

        addingTask.IsCompleted.Should().BeFalse();
        _processed.Count.Should().Be(1);

        _semaphore.Release();

        await addingTask;

        await buffer.Complete();
        bufferChannel.Writer.Complete();
        await bufferChannel.Reader.Completion;
        await batchReadTask;

        _processed.Should().HaveCount(events.Length / maxBatchSize + 1);
        _processed.SelectMany(x => x.Events.Segment).Select(x => x.Event)
            .Should()
            .BeEquivalentTo(events.Append(overBufferEvent), c => c.WithStrictOrdering());
    }

    private async Task RunSemaphoreRead(ChannelReader<Buffer<RedMessage>.Batch> reader)
    {
        await foreach (var e in reader.ReadAllAsync())
        {
            _processed.Add(e);

            if (_processed.Count == 1)
                await _semaphore.WaitAsync();
        }
    }
}