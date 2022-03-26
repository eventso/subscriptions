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
        public async Task AddingItemToFaultedBuffer_Throws()
        {
            var faultedBlock = new ActionBlock<Buffer<RedMessage>.Batch>(
                _ => throw new TestException(),
                new ExecutionDataflowBlockOptions { BoundedCapacity = 1 });

            var buffer = new Buffer<RedMessage>(
                maxBatchSize: 2,
                Timeout.InfiniteTimeSpan,
                faultedBlock,
                maxBufferSize: 6);

            foreach (var redEvent in _fixture.CreateMany<RedMessage>(2))
                await buffer.Add(redEvent, false, CancellationToken.None);

            Task.WaitAny(faultedBlock.Completion);

            Func<Task> act = () =>
                buffer.Add(_fixture.Create<RedMessage>(), false, CancellationToken.None);

            await act.Should().ThrowAsync<TestException>();
        }

        [Fact]
        public async Task AddingItemToBufferWithCompletedTarget_Throws()
        {
            var buffer = new Buffer<RedMessage>(
                maxBatchSize: 2,
                Timeout.InfiniteTimeSpan,
                _bufferBlock,
                maxBufferSize: 6);

            foreach (var redEvent in _fixture.CreateMany<RedMessage>(2))
                await buffer.Add(redEvent, false, CancellationToken.None);

            _bufferBlock.Complete();
            _bufferBlock.TryReceiveAll(out var _);
            await _bufferBlock.Completion;

            Func<Task> act = () =>
                buffer.Add(_fixture.Create<RedMessage>(), false, CancellationToken.None);

            await act.Should().ThrowAsync<InvalidOperationException>();
        }

        public sealed class TestException : Exception
        {
        }
    }
}