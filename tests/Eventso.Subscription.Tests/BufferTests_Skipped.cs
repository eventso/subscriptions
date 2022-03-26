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
    public sealed class BufferTests_Skipped
    {
        private readonly Fixture _fixture = new();
        private readonly BufferBlock<Buffer<RedMessage>.Batch> _targetBlock;

        public BufferTests_Skipped()
        {
            _targetBlock = new BufferBlock<Buffer<RedMessage>.Batch>();
        }

        [Fact]
        public async Task AddingSkippedItem_CorrectBatching()
        {
            const int maxBatchSize = 10;
            const int maxBufferSize = 10 * 3;
            const int eventsCount = maxBufferSize * 3 + 1;

            var buffer = new Buffer<RedMessage>(
                maxBatchSize,
                Timeout.InfiniteTimeSpan,
                _targetBlock,
                maxBufferSize);

            var events = _fixture.CreateMany<RedMessage>(eventsCount).ToArray();

            foreach (var @event in events)
                await buffer.Add(@event, skipped: true, CancellationToken.None);

            await buffer.TriggerSend(CancellationToken.None);

            _targetBlock.Complete();
            _targetBlock.TryReceiveAll(out var batches);

            batches.Should().HaveCount((int)Math.Ceiling(new decimal(eventsCount) / maxBufferSize));
            batches.SelectMany(x => x.Events).Select(x => x.Event)
                .Should()
                .BeEquivalentTo(events, c => c.WithStrictOrdering());
        }

        [Fact]
        public async Task AddingMixedItems_CorrectBatching()
        {
            const int maxBatchSize = 10;
            const int maxBufferSize = 10 * 3;
            const int eventsCount = maxBufferSize + 1;

            var buffer = new Buffer<RedMessage>(
                maxBatchSize,
                Timeout.InfiniteTimeSpan,
                _targetBlock,
                maxBufferSize);

            var events = _fixture.CreateMany<RedMessage>(eventsCount).ToArray();

            foreach (var @event in events.Take(maxBatchSize - 1))
                await buffer.Add(@event, skipped: false, CancellationToken.None);

            foreach (var @event in events.Skip(maxBatchSize - 1))
                await buffer.Add(@event, skipped: true, CancellationToken.None);

            await buffer.TriggerSend(CancellationToken.None);

            _targetBlock.Complete();
            _targetBlock.TryReceiveAll(out var batches);

            batches.Should().HaveCount(2);
            batches.SelectMany(x => x.Events).Select(x => x.Event)
                .Should()
                .BeEquivalentTo(events, c => c.WithStrictOrdering());

            batches[0].Events.Count.Should().Be(maxBufferSize);
        }

        [Fact]
        public async Task CheckingTimeoutOnTimeout_BatchCreated()
        {
            const int maxBatchSize = 10;
            const int batchTimeoutMs = 300;

            var buffer = new Buffer<RedMessage>(
                maxBatchSize,
                TimeSpan.FromMilliseconds(batchTimeoutMs),
                _targetBlock,
                maxBufferSize: maxBatchSize * 3);

            var events = _fixture.CreateMany<RedMessage>(maxBatchSize * 2 - 1).ToArray();

            foreach (var @event in events)
                await buffer.Add(@event, skipped: true, CancellationToken.None);

            await Task.Delay(batchTimeoutMs + 50);
            await buffer.CheckTimeout(CancellationToken.None);

            _targetBlock.Complete();

            var batches = new List<Buffer<RedMessage>.Batch>();

            while (!_targetBlock.Completion.IsCompleted)
            {
                _targetBlock.TryReceiveAll(out var items);
                if (items != null)
                    batches.AddRange(items);
            }

            batches.Should().ContainSingle()
                .Subject.Events.Select(x => x.Event)
                .Should()
                .BeEquivalentTo(events, c => c.WithStrictOrdering());
        }

        [Fact]
        public async Task AddingOnTimeout_BatchCreated()
        {
            const int maxBatchSize = 10;
            const int batchTimeoutMs = 300;

            var buffer = new Buffer<RedMessage>(
                maxBatchSize,
                TimeSpan.FromMilliseconds(batchTimeoutMs),
                _targetBlock,
                maxBufferSize: maxBatchSize * 3);

            var events = _fixture.CreateMany<RedMessage>(maxBatchSize * 2 - 5).ToArray();

            foreach (var @event in events.SkipLast(1))
                await buffer.Add(@event, skipped: true, CancellationToken.None);

            await Task.Delay(batchTimeoutMs + 50);

            await buffer.Add(events.Last(), skipped: true, CancellationToken.None);

            _targetBlock.TryReceiveAll(out var batches);

            batches.Should().ContainSingle()
                .Subject.Events.Select(x => x.Event)
                .Should()
                .BeEquivalentTo(events, c => c.WithStrictOrdering());
        }
    }
}