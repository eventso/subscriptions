using System.Threading.Channels;
using System.Threading.Tasks.Dataflow;

namespace Eventso.Subscription.Tests;

public sealed class BufferTests_Skipped
{
    private readonly Fixture _fixture = new();

    private readonly Channel<Buffer<RedMessage>.Batch> _bufferChannel =
        Channel.CreateUnbounded<Buffer<RedMessage>.Batch>();


    [Fact]
    public async Task AddingSkippedItem_CorrectBatching()
    {
        const int maxBatchSize = 10;
        const int maxBufferSize = 10 * 3;
        const int eventsCount = maxBufferSize * 3 + 1;

        using var buffer = new Buffer<RedMessage>(
            maxBatchSize,
            Timeout.InfiniteTimeSpan,
            _bufferChannel,
            maxBufferSize,
            CancellationToken.None);

        var events = _fixture.CreateMany<RedMessage>(eventsCount).ToArray();

        foreach (var @event in events)
            await buffer.Add(@event, skipped: true, CancellationToken.None);

        await buffer.Complete();

        _bufferChannel.Writer.Complete();

        var batches = new List<Buffer<RedMessage>.Batch>();
        while (_bufferChannel.Reader.TryRead(out var batch))
            batches.Add(batch);

        batches.Should().HaveCount((int)Math.Ceiling(new decimal(eventsCount) / maxBufferSize));
        batches.SelectMany(x => x.Events.Segment).Select(x => x.Event)
            .Should()
            .BeEquivalentTo(events, c => c.WithStrictOrdering());
    }

    [Fact]
    public async Task AddingMixedItems_CorrectBatching()
    {
        const int maxBatchSize = 10;
        const int maxBufferSize = 10 * 3;
        const int eventsCount = maxBufferSize + 1;

        using var buffer = new Buffer<RedMessage>(
            maxBatchSize,
            Timeout.InfiniteTimeSpan,
            _bufferChannel,
            maxBufferSize,
            CancellationToken.None);

        var events = _fixture.CreateMany<RedMessage>(eventsCount).ToArray();

        foreach (var @event in events.Take(maxBatchSize - 1))
            await buffer.Add(@event, skipped: false, CancellationToken.None);

        foreach (var @event in events.Skip(maxBatchSize - 1))
            await buffer.Add(@event, skipped: true, CancellationToken.None);

        await buffer.Complete();

        _bufferChannel.Writer.Complete();

        var batches = new List<Buffer<RedMessage>.Batch>();
        while (_bufferChannel.Reader.TryRead(out var batch))
            batches.Add(batch);

        batches.Should().HaveCount(2);
        batches.SelectMany(x => x.Events.Segment).Select(x => x.Event)
            .Should()
            .BeEquivalentTo(events, c => c.WithStrictOrdering());

        batches[0].Events.Count.Should().Be(maxBufferSize);
    }
}