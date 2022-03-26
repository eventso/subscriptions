using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AutoFixture;
using AutoFixture.AutoNSubstitute;
using Eventso.Subscription.Configurations;
using Eventso.Subscription.Observing.Batch;
using FluentAssertions;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Xunit;

namespace Eventso.Subscription.Tests
{
    public sealed class BatchEventObserverTests
    {
        private readonly Fixture _fixture;
        private readonly BatchEventObserver<IEvent> _observer;
        private readonly TestConsumer _consumer;
        private readonly List<IReadOnlyCollection<IEvent>> _handledBatches = new();
        private readonly IMessageHandlersRegistry _handlersRegistry;

        public BatchEventObserverTests()
        {
            _fixture = new Fixture();
            _fixture.Customize(new AutoNSubstituteCustomization {ConfigureMembers = true});

            _handlersRegistry = _fixture.Create<IMessageHandlersRegistry>();

            var batchHandler = Substitute.For<IBatchHandler<IEvent>>();
            batchHandler.Handle(default, default)
                .ReturnsForAnyArgs(Task.CompletedTask)
                .AndDoes(c => _handledBatches.Add(c.Arg<IReadOnlyList<IEvent>>().ToArray()));

            _consumer = new TestConsumer();

            _observer = new BatchEventObserver<IEvent>(
                new BatchConfiguration { BatchTriggerTimeout = TimeSpan.FromDays(1), MaxBatchSize = 10 },
                batchHandler,
                _consumer,
                _handlersRegistry);
        }

        [Fact]
        public async Task ObservingEvents_AllAcknowledged()
        {
            _handlersRegistry.ContainsHandlersFor(default, out _)
                .ReturnsForAnyArgs(true);
            _fixture.Inject(DeserializationStatus.Success);

            var events = _fixture.CreateMany<IEvent>(56).ToArray();

            foreach (var @event in events)
                await _observer.OnEventAppeared(@event, CancellationToken.None);

            await _observer.Complete(CancellationToken.None);

            _consumer.Acks.Should()
                .BeEquivalentTo(
                    events,
                    c => c.WithStrictOrdering());

            _handledBatches.SelectMany(x => x)
                .Should().HaveSameCount(events);
        }

        [Fact]
        public async Task ObservingSkippedWithoutHandlerEvents_AllAcknowledged()
        {
            _handlersRegistry.ContainsHandlersFor(default, out _)
                .ReturnsForAnyArgs(false);

            var events = _fixture.CreateMany<IEvent>(56).ToArray();

            foreach (var @event in events)
                await _observer.OnEventAppeared(@event, CancellationToken.None);

            await _observer.Complete(CancellationToken.None);

            _consumer.Acks.Should()
                .BeEquivalentTo(
                    events,
                    c => c.WithStrictOrdering());

            _handledBatches.Should().BeEmpty();
        }

        [Fact]
        public async Task ObservingSkippedByDeserializerEvents_AllAcknowledged()
        {
            _handlersRegistry.ContainsHandlersFor(default, out _)
                .ReturnsForAnyArgs(true);

            _fixture.Inject(DeserializationStatus.Skipped);

            var events = _fixture.CreateMany<IEvent>(56).ToArray();

            foreach (var @event in events)
                await _observer.OnEventAppeared(@event, CancellationToken.None);

            await _observer.Complete(CancellationToken.None);

            _consumer.Acks.Should()
                .BeEquivalentTo(
                    events,
                    c => c.WithStrictOrdering());

            _handledBatches.Should().BeEmpty();
        }

        [Fact]
        public async Task ObservingSkippedUnknownTypeEvents_AllAcknowledged()
        {
            _handlersRegistry.ContainsHandlersFor(default, out _)
                .ReturnsForAnyArgs(true);

            _fixture.Inject(DeserializationStatus.UnknownType);

            var events = _fixture.CreateMany<IEvent>(56).ToArray();

            foreach (var @event in events)
                await _observer.OnEventAppeared(@event, CancellationToken.None);

            await _observer.Complete(CancellationToken.None);

            _consumer.Acks.Should()
                .BeEquivalentTo(
                    events,
                    c => c.WithStrictOrdering());

            _handledBatches.Should().BeEmpty();
        }

        [Fact]
        public async Task ObservingMixedEvents_AllAcknowledged()
        {
            _handlersRegistry.ContainsHandlersFor(default, out _)
                .ReturnsForAnyArgs(true);

            var skippedEvents = Enumerable.Repeat(0, 25)
                .Select(_ =>
                {
                    var msg = Substitute.For<IEvent>();
                    msg.DeserializationResult.Returns(DeserializationStatus.Skipped);
                    return msg;
                }).ToArray();

            var successEvents = Enumerable.Repeat(0, 25)
                .Select(_ =>
                {
                    var msg = Substitute.For<IEvent>();
                    msg.DeserializationResult.Returns(DeserializationStatus.Success);
                    msg.GetMessage().Returns(new object());

                    return msg;
                }).ToArray();

            var events = skippedEvents.Concat(successEvents)
                .OrderBy(_ => Guid.NewGuid())
                .ToArray();

            foreach (var @event in events)
                await _observer.OnEventAppeared(@event, CancellationToken.None);

            await _observer.Complete(CancellationToken.None);

            _consumer.Acks.Should()
                .BeEquivalentTo(
                    events,
                    c => c.WithStrictOrdering());

            _handledBatches.SelectMany(x => x).Should()
                .BeEquivalentTo(
                    successEvents,
                    c => c.WithStrictOrdering());
        }

        [Fact]
        public async Task ObservingCompleted_Throws()
        {
            var @event = _fixture.Create<IEvent>();

            await _observer.Complete(CancellationToken.None);

            Func<Task> act = () => _observer.OnEventAppeared(
                @event, CancellationToken.None);

            await act.Should()
                .ThrowAsync<InvalidOperationException>()
                .WithMessage("* completed *");
        }

        [Fact]
        public async Task ObservingFaulted_Throws()
        {
            _handlersRegistry.ContainsHandlersFor(default, out _)
                .ReturnsForAnyArgs(true);
            _fixture.Inject(DeserializationStatus.Success);

            using var semaphore = new SemaphoreSlim(0);

            var batchHandler = Substitute.For<IBatchHandler<IEvent>>();
            batchHandler.Handle(default, default)
                .ThrowsForAnyArgs(new TestException())
                .AndDoes(_ => semaphore.Release());

            const int batchCount = 2;

            var observer = new BatchEventObserver<IEvent>(
                new BatchConfiguration { BatchTriggerTimeout = TimeSpan.FromDays(1), MaxBatchSize = batchCount },
                batchHandler,
                _consumer,
                _handlersRegistry);

            var @event = _fixture.Create<IEvent>();

            for (var i = 0; i < batchCount; i++)
                await observer.OnEventAppeared(@event, CancellationToken.None);

            await semaphore.WaitAsync();
            await Task.Delay(100);

            Func<Task> act = () => observer.OnEventAppeared(
                @event, CancellationToken.None);

            await act.Should().ThrowAsync<TestException>();
        }

        [Fact]
        public async Task ObservingDisposed_Throws()
        {
            var observer = new BatchEventObserver<IEvent>(
                new BatchConfiguration { BatchTriggerTimeout = TimeSpan.FromDays(1), MaxBatchSize = 2 },
                Substitute.For<IBatchHandler<IEvent>>(),
                _consumer,
                _handlersRegistry);

            var @event = _fixture.Create<IEvent>();

            observer.Dispose();

            Func<Task> act = () => observer.OnEventAppeared(
                @event, CancellationToken.None);

            Func<Task> actComplete = () => observer.Complete(CancellationToken.None);

            await act.Should().ThrowAsync<ObjectDisposedException>();
            await actComplete.Should().ThrowAsync<ObjectDisposedException>();
        }

        [Fact]
        public async Task CompletingFaulted_Throws()
        {
            _handlersRegistry.ContainsHandlersFor(default, out _)
                .ReturnsForAnyArgs(true);
            _fixture.Inject(DeserializationStatus.Success);

            using var semaphore = new SemaphoreSlim(0);

            var batchHandler = Substitute.For<IBatchHandler<IEvent>>();
            batchHandler.Handle(default, default)
                .ThrowsForAnyArgs(new TestException())
                .AndDoes(_ => semaphore.Release());

            const int batchCount = 2;

            var observer = new BatchEventObserver<IEvent>(
                new BatchConfiguration { BatchTriggerTimeout = TimeSpan.FromDays(1), MaxBatchSize = batchCount },
                batchHandler,
                _consumer,
                _handlersRegistry);

            var @event = _fixture.Create<IEvent>();

            for (var i = 0; i < batchCount; i++)
                await observer.OnEventAppeared(@event, CancellationToken.None);

            await semaphore.WaitAsync();
            await Task.Delay(100);

            Func<Task> act = () => observer.Complete(CancellationToken.None);

            await act.Should().ThrowAsync<TestException>();
        }

        [Fact]
        public async Task DisposingFaulted_Disposed()
        {
            _handlersRegistry.ContainsHandlersFor(default, out _)
                .ReturnsForAnyArgs(true);
            _fixture.Inject(DeserializationStatus.Success);

            using var semaphore = new SemaphoreSlim(0);

            var batchHandler = Substitute.For<IBatchHandler<IEvent>>();
            batchHandler.Handle(default, default)
                .ThrowsForAnyArgs(new TestException())
                .AndDoes(_ => semaphore.Release());

            const int batchCount = 2;

            var observer = new BatchEventObserver<IEvent>(
                new BatchConfiguration { BatchTriggerTimeout = TimeSpan.FromDays(1), MaxBatchSize = batchCount },
                batchHandler,
                _consumer,
                _handlersRegistry);

            var @event = _fixture.Create<IEvent>();

            for (var i = 0; i < batchCount; i++)
                await observer.OnEventAppeared(@event, CancellationToken.None);

            await semaphore.WaitAsync();
            await Task.Delay(100);

            observer.Dispose();
            observer.Dispose();
        }

        [Fact]
        public async Task DisposingTwice_Disposed()
        {
            _handlersRegistry.ContainsHandlersFor(default, out _)
                .ReturnsForAnyArgs(true);
            _fixture.Inject(DeserializationStatus.Success);

            const int batchCount = 2;

            var observer = new BatchEventObserver<IEvent>(
                new BatchConfiguration { BatchTriggerTimeout = TimeSpan.FromDays(1), MaxBatchSize = batchCount },
                Substitute.For<IBatchHandler<IEvent>>(),
                _consumer,
                _handlersRegistry);

            var @event = _fixture.Create<IEvent>();

            for (var i = 0; i < batchCount; i++)
                await observer.OnEventAppeared(@event, CancellationToken.None);

            observer.Dispose();
            observer.Dispose();
        }

        public sealed class TestException : Exception
        {
        }

        private sealed class TestConsumer : IConsumer<IEvent>
        {
            public readonly List<IEvent> Acks = new();
            public readonly CancellationTokenSource CancellationTokenSource = new();

            public CancellationToken CancellationToken => CancellationTokenSource.Token;

            public string Subscription { get; } = "Some";

            public void Acknowledge(in IEvent events) =>
                Acks.Add(events);

            public void Acknowledge(IReadOnlyList<IEvent> events) =>
                Acks.AddRange(events);

            public void Cancel() => CancellationTokenSource.Cancel();
        }
    }
}