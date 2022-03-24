using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using AutoFixture;
using Eventso.Subscription.Observing.Batch;
using FluentAssertions;
using Xunit;

namespace Eventso.Subscription.Tests
{
    public sealed class BufferTests_NotSkipped : IDisposable
    {
        private readonly Fixture _fixture = new();
        private readonly BufferBlock<Buffer<RedEvent>.Batch> _targetBlock;
        private readonly SemaphoreSlim _semaphore = new(0);
        private readonly List<Buffer<RedEvent>.Batch> _processed = new();
        private readonly ActionBlock<Buffer<RedEvent>.Batch> _semaphoreBlock;

        public BufferTests_NotSkipped()
        {
            _targetBlock = new BufferBlock<Buffer<RedEvent>.Batch>();

            _semaphoreBlock = new ActionBlock<Buffer<RedEvent>.Batch>(
                e =>
                {
                    _processed.Add(e);
                    return _processed.Count == 1
                        ? _semaphore.WaitAsync()
                        : Task.CompletedTask;
                },
                new ExecutionDataflowBlockOptions()
                {
                    BoundedCapacity = 1,
                    MaxDegreeOfParallelism = 1
                });
        }

        public void Dispose()
        {
            _semaphore.Dispose();
        }

        [Theory]
        [InlineData(4, 12)]
        [InlineData(2, 5)]
        [InlineData(10, 5)]
        public async Task AddingItem_CorrectBatching(
            int maxBatchSize, int eventsCount)
        {
            var buffer = new Buffer<RedEvent>(
                maxBatchSize,
                Timeout.InfiniteTimeSpan,
                _targetBlock,
                maxBufferSize: maxBatchSize * 3);

            var events = _fixture.CreateMany<RedEvent>(eventsCount).ToArray();

            foreach (var @event in events)
                await buffer.Add(@event, skipped: false, CancellationToken.None);

            await buffer.TriggerSend(CancellationToken.None);

            _targetBlock.Complete();
            _targetBlock.TryReceiveAll(out var batches);

            batches.Should().HaveCount((int)Math.Ceiling(new decimal(eventsCount) / maxBatchSize));
            batches.SelectMany(x => x.Messages).Select(x => x.Message)
                .Should()
                .BeEquivalentTo(events, c => c.WithStrictOrdering());
        }

        [Fact]
        public async Task AddingItem_BatchesProcessedInStreamedWay()
        {
            const int maxBufferSize = 3;
            var buffer = new Buffer<RedEvent>(
                maxBufferSize,
                Timeout.InfiniteTimeSpan,
                _targetBlock,
                maxBufferSize: maxBufferSize * 3);

            var events = _fixture.CreateMany<RedEvent>(maxBufferSize * 3).ToArray();

            for (var iteration = 0; iteration < 2; iteration++)
            {
                for (var i = 0; i < maxBufferSize; i++)
                    await buffer.Add(events[i + (iteration * maxBufferSize)], skipped: false, CancellationToken.None);

                await Task.Delay(100);

                _targetBlock.TryReceiveAll(out var batches);

                using var batch = batches.Should().ContainSingle().Subject.Messages;
                batch.Segment.Should().HaveCount(maxBufferSize);
                batch.Segment.Select(x => x.Message)
                    .Should()
                    .BeEquivalentTo(
                        events.Skip(iteration * maxBufferSize).Take(maxBufferSize),
                        c => c.WithStrictOrdering());
            }
        }

        [Fact]
        public async Task CheckingTimeoutOnTimeout_BatchCreated()
        {
            const int maxBatchSize = 10;
            const int batchTimeoutMs = 300;

            var buffer = new Buffer<RedEvent>(
                maxBatchSize,
                TimeSpan.FromMilliseconds(batchTimeoutMs),
                _targetBlock,
                maxBufferSize: maxBatchSize * 3);

            var events = _fixture.CreateMany<RedEvent>(maxBatchSize * 2 - 1).ToArray();

            foreach (var @event in events)
                await buffer.Add(@event, skipped: false, CancellationToken.None);

            await Task.Delay(batchTimeoutMs + 50);
            await buffer.CheckTimeout(CancellationToken.None);

            _targetBlock.Complete();

            var batches = new List<Buffer<RedEvent>.Batch>();

            while (!_targetBlock.Completion.IsCompleted)
            {
                _targetBlock.TryReceiveAll(out var items);
                if (items != null)
                    batches.AddRange(items);
            }

            batches.Should().HaveCount(2);
            batches.SelectMany(x => x.Messages).Select(x => x.Message)
                .Should()
                .BeEquivalentTo(events, c => c.WithStrictOrdering());
        }

        [Fact]
        public async Task AddingOnTimeout_BatchCreated()
        {
            const int maxBatchSize = 10;
            const int batchTimeoutMs = 300;

            var buffer = new Buffer<RedEvent>(
                maxBatchSize,
                TimeSpan.FromMilliseconds(batchTimeoutMs),
                _targetBlock,
                maxBufferSize: maxBatchSize * 3);

            var events = _fixture.CreateMany<RedEvent>(maxBatchSize * 2 - 5).ToArray();

            foreach (var @event in events.SkipLast(1))
                await buffer.Add(@event, skipped: false, CancellationToken.None);

            await Task.Delay(batchTimeoutMs + 50);

            await buffer.Add(events.Last(), skipped: false, CancellationToken.None);

            _targetBlock.TryReceiveAll(out var batches);

            batches.Should().HaveCount(2);
            batches.SelectMany(x => x.Messages).Select(x => x.Message)
                .Should()
                .BeEquivalentTo(events, c => c.WithStrictOrdering());
        }

        [Fact]
        public async Task TriggeringSend_MessagesFlushed()
        {
            const int maxBatchSize = 5000;
            const int eventsCount = 10;

            var buffer = new Buffer<RedEvent>(
                maxBatchSize,
                Timeout.InfiniteTimeSpan,
                _targetBlock,
                maxBufferSize: maxBatchSize * 3);

            var events = _fixture.CreateMany<RedEvent>(eventsCount).ToArray();

            foreach (var @event in events)
                await buffer.Add(@event, skipped: false, CancellationToken.None);

            await buffer.TriggerSend(CancellationToken.None);

            _targetBlock.TryReceiveAll(out var batches);

            var batch = batches.Should().ContainSingle().Subject;
            batch.Messages.Select(x => x.Message)
                .Should()
                .BeEquivalentTo(events, c => c.WithStrictOrdering());
        }


        [Fact]
        public async Task AddingItemToBlockedTarget_ItemAdded()
        {
            const int maxBatchSize = 10;
            const int eventsCount = 19;

            var buffer = new Buffer<RedEvent>(
                maxBatchSize,
                Timeout.InfiniteTimeSpan,
                _semaphoreBlock,
                maxBufferSize: maxBatchSize * 3);

            var events = _fixture.CreateMany<RedEvent>(eventsCount).ToArray();

            foreach (var @event in events)
                await buffer.Add(@event, skipped: false, CancellationToken.None);

            var completeTask = buffer.TriggerSend(CancellationToken.None);

            await Task.Delay(100);

            completeTask.IsCompleted.Should().BeFalse();
            _processed.Count.Should().Be(1);

            _semaphore.Release();

            await completeTask;
            _semaphoreBlock.Complete();
            await _semaphoreBlock.Completion;

            _processed.Should().HaveCount((int)Math.Ceiling((double)events.Length / maxBatchSize));
            _processed.SelectMany(x => x.Messages).Select(x => x.Message)
                .Should()
                .BeEquivalentTo(events, c => c.WithStrictOrdering());
        }

        [Fact]
        public async Task AddingItemOverBufferCapacityToBlockedTarget_AddingBlocked()
        {
            const int maxBatchSize = 10;
            const int eventsCount = 19;

            var buffer = new Buffer<RedEvent>(
                maxBatchSize,
                Timeout.InfiniteTimeSpan,
                _semaphoreBlock,
                maxBufferSize: maxBatchSize * 3);

            var events = _fixture.CreateMany<RedEvent>(eventsCount).ToArray();

            foreach (var @event in events)
                await buffer.Add(@event, skipped: false, CancellationToken.None);


            var overBufferEvent = _fixture.Create<RedEvent>();
            var addingTask = buffer.Add(overBufferEvent, skipped: false, CancellationToken.None);

            await Task.Delay(100);

            addingTask.IsCompleted.Should().BeFalse();
            _processed.Count.Should().Be(1);

            _semaphore.Release();

            await addingTask;

            await buffer.TriggerSend(CancellationToken.None);
            _semaphoreBlock.Complete();
            await _semaphoreBlock.Completion;

            _processed.Should().HaveCount(events.Length / maxBatchSize + 1);
            _processed.SelectMany(x => x.Messages).Select(x => x.Message)
                .Should()
                .BeEquivalentTo(events.Append(overBufferEvent), c => c.WithStrictOrdering());
        }
    }
}