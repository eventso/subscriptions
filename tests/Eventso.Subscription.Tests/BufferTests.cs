using System.Threading.Channels;

namespace Eventso.Subscription.Tests;

public sealed class BufferTests
{
    private readonly Fixture _fixture = new();

    private readonly Channel<Buffer<RedMessage>.Batch> _bufferChannel =
        Channel.CreateUnbounded<Buffer<RedMessage>.Batch>();

    [Fact]
    public async Task AddingItemToEmptyCompletedBuffer_Throws()
    {
        using var buffer = new Buffer<RedMessage>(
            5,
            Timeout.InfiniteTimeSpan,
            _bufferChannel,
            15,
            CancellationToken.None);

        await buffer.Complete();

        var act = () => buffer.Add(_fixture.Create<RedMessage>(), false, CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("* completed*");
    }

    [Fact]
    public async Task AddingItemToFullCompletedBuffer_Throws()
    {
        using var buffer = new Buffer<RedMessage>(
            2,
            Timeout.InfiniteTimeSpan,
            Channel.CreateBounded<Buffer<RedMessage>.Batch>(1),
            15,
            CancellationToken.None);

        foreach (var redEvent in _fixture.CreateMany<RedMessage>(2))
            await buffer.Add(redEvent, false, CancellationToken.None);

        await buffer.Complete();

        var act = () => buffer.Add(_fixture.Create<RedMessage>(), false, CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("* completed*");
    }

    [Fact]
    public async Task AddingItemToFaultedBuffer_Throws()
    {
        var faultedChannel = Channel.CreateBounded<Buffer<RedMessage>.Batch>(1);
        
        using var buffer = new Buffer<RedMessage>(
            2,
            Timeout.InfiniteTimeSpan,
            faultedChannel,
            15,
            CancellationToken.None);

        foreach (var redEvent in _fixture.CreateMany<RedMessage>(2))
            await buffer.Add(redEvent, false, CancellationToken.None);

        await Task.Delay(100);

        faultedChannel.Writer.Complete(new TestException());

        while (faultedChannel.Reader.TryRead(out _)) ;

        var act = () =>
            buffer.Add(_fixture.Create<RedMessage>(), false, CancellationToken.None);

        await act.Should().ThrowAsync<TestException>();
    }

    [Fact]
    public async Task AddingItemToCancelledBuffer_Throws()
    {
        using var cts = new CancellationTokenSource();

        using var buffer = new Buffer<RedMessage>(
            2,
            Timeout.InfiniteTimeSpan,
            _bufferChannel,
            15,
            cts.Token);

        foreach (var redEvent in _fixture.CreateMany<RedMessage>(2))
            await buffer.Add(redEvent, false, CancellationToken.None);

        cts.Cancel();

        await Task.Delay(50);

        var act = () =>
            buffer.Add(_fixture.Create<RedMessage>(), false, CancellationToken.None);

        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task DisposingCancelledBuffer_Disposed()
    {
        using var cts = new CancellationTokenSource();

        var buffer = new Buffer<RedMessage>(
            2,
            Timeout.InfiniteTimeSpan,
            _bufferChannel,
            15,
            cts.Token);

        foreach (var redEvent in _fixture.CreateMany<RedMessage>(2))
            await buffer.Add(redEvent, false, CancellationToken.None);

        cts.Cancel();
        await Task.Delay(50);

        buffer.Dispose();
    }

    [Fact]
    public async Task CompletingCancelledBuffer_Throws()
    {
        using var cts = new CancellationTokenSource();

        var buffer = new Buffer<RedMessage>(
            2,
            Timeout.InfiniteTimeSpan,
            _bufferChannel,
            15,
            cts.Token);

        foreach (var redEvent in _fixture.CreateMany<RedMessage>(2))
            await buffer.Add(redEvent, false, CancellationToken.None);

        cts.Cancel();
        await Task.Delay(50);

        var act = () => buffer.Complete();

        await act.Should().ThrowAsync<TaskCanceledException>();
    }

    [Fact]
    public async Task MultipleDisposingBuffer_Disposed()
    {
        var buffer = new Buffer<RedMessage>(
            2,
            Timeout.InfiniteTimeSpan,
            _bufferChannel,
            15,
            CancellationToken.None);

        foreach (var redEvent in _fixture.CreateMany<RedMessage>(2))
            await buffer.Add(redEvent, false, CancellationToken.None);

        buffer.Dispose();
        buffer.Dispose();
    }

    [Fact]
    public async Task AddingItemToBufferWithCompletedTarget_Throws()
    {
        using var buffer = new Buffer<RedMessage>(
            2,
            Timeout.InfiniteTimeSpan,
            _bufferChannel,
            15,
            CancellationToken.None);

        foreach (var redEvent in _fixture.CreateMany<RedMessage>(2))
            await buffer.Add(redEvent, false, CancellationToken.None);

        _bufferChannel.Writer.Complete();
        while (_bufferChannel.Reader.TryRead(out _)) ;

        await _bufferChannel.Reader.Completion;

        var act = () =>
            buffer.Add(_fixture.Create<RedMessage>(), false, CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>();
    }

    [Fact]
    public async Task AddingItemToDisposedBuffer_Throws()
    {
        using var buffer = new Buffer<RedMessage>(
            2,
            Timeout.InfiniteTimeSpan,
            _bufferChannel,
            15,
            CancellationToken.None);

        foreach (var redEvent in _fixture.CreateMany<RedMessage>(2))
            await buffer.Add(redEvent, false, CancellationToken.None);

        buffer.Dispose();

        var act = () =>
            buffer.Add(_fixture.Create<RedMessage>(), false, CancellationToken.None);

        await act.Should().ThrowAsync<ObjectDisposedException>();
    }

    [Fact]
    public async Task CompletingDisposedBuffer_Throws()
    {
        using var buffer = new Buffer<RedMessage>(
            2,
            Timeout.InfiniteTimeSpan,
            _bufferChannel,
            15,
            CancellationToken.None);

        foreach (var redEvent in _fixture.CreateMany<RedMessage>(2))
            await buffer.Add(redEvent, false, CancellationToken.None);

        buffer.Dispose();

        var act = () => buffer.Complete();

        await act.Should().ThrowAsync<ObjectDisposedException>();
    }

    public sealed class TestException : Exception
    {
    }
}