using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using AutoFixture;
using Eventso.Subscription.Observing.Batch;
using FluentAssertions;
using Xunit;

namespace Eventso.Subscription.Tests
{
    public sealed class BufferTests
    {
        private readonly Fixture _fixture = new();
        private readonly BufferBlock<Buffer<RedMessage>.Batch> _bufferBlock = new();

        [Fact]
        public async Task AddingItemToEmptyCompletedBuffer_Throws()
        {
            using var buffer = new Buffer<RedMessage>(
                5,
                Timeout.InfiniteTimeSpan,
                _bufferBlock,
                15,
                CancellationToken.None);

            await buffer.Complete();

            Func<Task> act = () => buffer.Add(_fixture.Create<RedMessage>(), false, CancellationToken.None);

            await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("* completed*");
        }

        [Fact]
        public async Task AddingItemToFullCompletedBuffer_Throws()
        {
            using var buffer = new Buffer<RedMessage>(
                2,
                Timeout.InfiniteTimeSpan,
                new ActionBlock<Buffer<RedMessage>.Batch>(
                    _ => Task.Delay(200),
                    new ExecutionDataflowBlockOptions() {BoundedCapacity = 1}),
                15,
                CancellationToken.None);

            foreach (var redEvent in _fixture.CreateMany<RedMessage>(20))
                await buffer.Add(redEvent, false, CancellationToken.None);

            await buffer.Complete();

            Func<Task> act = () => buffer.Add(_fixture.Create<RedMessage>(), false, CancellationToken.None);

            await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("* completed*");
        }

        [Fact]
        public async Task AddingItemToFaultedBuffer_Throws()
        {
            var faultedBlock = new ActionBlock<Buffer<RedMessage>.Batch>(
                _ => throw new TestException(),
                new ExecutionDataflowBlockOptions() {BoundedCapacity = 1});

            using var buffer = new Buffer<RedMessage>(
                2,
                Timeout.InfiniteTimeSpan,
                faultedBlock,
                15,
                CancellationToken.None);

            foreach (var redEvent in _fixture.CreateMany<RedMessage>(2))
                await buffer.Add(redEvent, false, CancellationToken.None);

            Task.WaitAny(faultedBlock.Completion);

            Func<Task> act = () =>
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
                _bufferBlock,
                15,
                cts.Token);

            foreach (var redEvent in _fixture.CreateMany<RedMessage>(2))
                await buffer.Add(redEvent, false, CancellationToken.None);

            cts.Cancel();

            await Task.Delay(50);

            Func<Task> act = () =>
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
                _bufferBlock,
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
                _bufferBlock,
                15,
                cts.Token);

            foreach (var redEvent in _fixture.CreateMany<RedMessage>(2))
                await buffer.Add(redEvent, false, CancellationToken.None);

            cts.Cancel();
            await Task.Delay(50);

            Func<Task> act = () => buffer.Complete();

            await act.Should().ThrowAsync<TaskCanceledException>();
        }

        [Fact]
        public async Task MultipleDisposingBuffer_Disposed()
        {
            var buffer = new Buffer<RedMessage>(
                2,
                Timeout.InfiniteTimeSpan,
                _bufferBlock,
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
                _bufferBlock,
                15,
                CancellationToken.None);

            foreach (var redEvent in _fixture.CreateMany<RedMessage>(2))
                await buffer.Add(redEvent, false, CancellationToken.None);

            _bufferBlock.Complete();
            await _bufferBlock.Completion;

            Func<Task> act = () =>
                buffer.Add(_fixture.Create<RedMessage>(), false, CancellationToken.None);

            await act.Should().ThrowAsync<InvalidOperationException>();
        }

        [Fact]
        public async Task AddingItemToDisposedBuffer_Throws()
        {
            using var buffer = new Buffer<RedMessage>(
                2,
                Timeout.InfiniteTimeSpan,
                _bufferBlock,
                15,
                CancellationToken.None);

            foreach (var redEvent in _fixture.CreateMany<RedMessage>(2))
                await buffer.Add(redEvent, false, CancellationToken.None);

            buffer.Dispose();

            Func<Task> act = () =>
                buffer.Add(_fixture.Create<RedMessage>(), false, CancellationToken.None);

            await act.Should().ThrowAsync<ObjectDisposedException>();
        }

        [Fact]
        public async Task CompletingDisposedBuffer_Throws()
        {
            using var buffer = new Buffer<RedMessage>(
                2,
                Timeout.InfiniteTimeSpan,
                _bufferBlock,
                15,
                CancellationToken.None);

            foreach (var redEvent in _fixture.CreateMany<RedMessage>(2))
                await buffer.Add(redEvent, false, CancellationToken.None);

            buffer.Dispose();

            Func<Task> act = () => buffer.Complete();

            await act.Should().ThrowAsync<ObjectDisposedException>();
        }

        public sealed class TestException : Exception
        {
        }
    }
}