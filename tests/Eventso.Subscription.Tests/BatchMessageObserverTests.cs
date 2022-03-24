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
    public sealed class BatchMessageObserverTests
    {
        private readonly Fixture _fixture;
        private readonly BatchMessageObserver<IMessage> _observer;
        private readonly TestConsumer _consumer;
        private readonly List<IReadOnlyCollection<IMessage>> _handledBatches = new();
        private readonly IMessageHandlersRegistry _handlersRegistry;

        public BatchMessageObserverTests()
        {
            _fixture = new Fixture();
            _fixture.Customize(new AutoNSubstituteCustomization {ConfigureMembers = true});

            _handlersRegistry = _fixture.Create<IMessageHandlersRegistry>();

            var batchHandler = Substitute.For<IBatchHandler<IMessage>>();
            batchHandler.Handle(default, default)
                .ReturnsForAnyArgs(Task.CompletedTask)
                .AndDoes(c => _handledBatches.Add(c.Arg<IReadOnlyList<IMessage>>().ToArray()));

            _consumer = new TestConsumer();

            _observer = new BatchMessageObserver<IMessage>(
                new BatchConfiguration { BatchTriggerTimeout = TimeSpan.FromDays(1), MaxBatchSize = 10 },
                batchHandler,
                _consumer,
                _handlersRegistry);
        }

        [Fact]
        public async Task ObservingMessages_AllAcknowledged()
        {
            _handlersRegistry.ContainsHandlersFor(default, out _)
                .ReturnsForAnyArgs(true);
            _fixture.Inject(DeserializationStatus.Success);

            var messages = _fixture.CreateMany<IMessage>(56).ToArray();

            foreach (var message in messages)
                await _observer.OnMessageAppeared(message, CancellationToken.None);

            await _observer.Complete(CancellationToken.None);

            _consumer.Acks.Should()
                .BeEquivalentTo(
                    messages,
                    c => c.WithStrictOrdering());

            _handledBatches.SelectMany(x => x)
                .Should().HaveSameCount(messages);
        }

        [Fact]
        public async Task ObservingSkippedWithoutHandlerMessages_AllAcknowledged()
        {
            _handlersRegistry.ContainsHandlersFor(default, out _)
                .ReturnsForAnyArgs(false);

            var messages = _fixture.CreateMany<IMessage>(56).ToArray();

            foreach (var message in messages)
                await _observer.OnMessageAppeared(message, CancellationToken.None);

            await _observer.Complete(CancellationToken.None);

            _consumer.Acks.Should()
                .BeEquivalentTo(
                    messages,
                    c => c.WithStrictOrdering());

            _handledBatches.Should().BeEmpty();
        }

        [Fact]
        public async Task ObservingSkippedByDeserializerMessages_AllAcknowledged()
        {
            _handlersRegistry.ContainsHandlersFor(default, out _)
                .ReturnsForAnyArgs(true);

            _fixture.Inject(DeserializationStatus.Skipped);

            var messages = _fixture.CreateMany<IMessage>(56).ToArray();

            foreach (var message in messages)
                await _observer.OnMessageAppeared(message, CancellationToken.None);

            await _observer.Complete(CancellationToken.None);

            _consumer.Acks.Should()
                .BeEquivalentTo(
                    messages,
                    c => c.WithStrictOrdering());

            _handledBatches.Should().BeEmpty();
        }

        [Fact]
        public async Task ObservingSkippedUnknownTypeMessages_AllAcknowledged()
        {
            _handlersRegistry.ContainsHandlersFor(default, out _)
                .ReturnsForAnyArgs(true);

            _fixture.Inject(DeserializationStatus.UnknownType);

            var messages = _fixture.CreateMany<IMessage>(56).ToArray();

            foreach (var message in messages)
                await _observer.OnMessageAppeared(message, CancellationToken.None);

            await _observer.Complete(CancellationToken.None);

            _consumer.Acks.Should()
                .BeEquivalentTo(
                    messages,
                    c => c.WithStrictOrdering());

            _handledBatches.Should().BeEmpty();
        }

        [Fact]
        public async Task ObservingMixedMessages_AllAcknowledged()
        {
            _handlersRegistry.ContainsHandlersFor(default, out _)
                .ReturnsForAnyArgs(true);

            var skippedMessages = Enumerable.Repeat(0, 25)
                .Select(_ =>
                {
                    var msg = Substitute.For<IMessage>();
                    msg.DeserializationResult.Returns(DeserializationStatus.Skipped);
                    return msg;
                }).ToArray();

            var payloadMessages = Enumerable.Repeat(0, 25)
                .Select(_ =>
                {
                    var msg = Substitute.For<IMessage>();
                    msg.DeserializationResult.Returns(DeserializationStatus.Success);
                    msg.GetPayload().Returns(new object());

                    return msg;
                }).ToArray();

            var messages = skippedMessages.Concat(payloadMessages)
                .OrderBy(_ => Guid.NewGuid())
                .ToArray();

            foreach (var message in messages)
                await _observer.OnMessageAppeared(message, CancellationToken.None);

            await _observer.Complete(CancellationToken.None);

            _consumer.Acks.Should()
                .BeEquivalentTo(
                    messages,
                    c => c.WithStrictOrdering());

            _handledBatches.SelectMany(x => x).Should()
                .BeEquivalentTo(
                    payloadMessages,
                    c => c.WithStrictOrdering());
        }

        [Fact]
        public async Task ObservingCompleted_Throws()
        {
            var message = _fixture.Create<IMessage>();

            await _observer.Complete(CancellationToken.None);

            Func<Task> act = () => _observer.OnMessageAppeared(
                message, CancellationToken.None);

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

            using var sempahore = new SemaphoreSlim(0);

            var batchHandler = Substitute.For<IBatchHandler<IMessage>>();
            batchHandler.Handle(default, default)
                .ThrowsForAnyArgs(new TestException())
                .AndDoes(_ => sempahore.Release());

            const int batchCount = 2;

            var observer = new BatchMessageObserver<IMessage>(
                new BatchConfiguration { BatchTriggerTimeout = TimeSpan.FromDays(1), MaxBatchSize = batchCount },
                batchHandler,
                _consumer,
                _handlersRegistry);

            var message = _fixture.Create<IMessage>();

            for (var i = 0; i < batchCount; i++)
                await observer.OnMessageAppeared(message, CancellationToken.None);

            await sempahore.WaitAsync();
            await Task.Delay(100);

            Func<Task> act = () => observer.OnMessageAppeared(
                message, CancellationToken.None);

            await act.Should().ThrowAsync<TestException>();
        }

        [Fact]
        public async Task ObservingDisposed_Throws()
        {
            var observer = new BatchMessageObserver<IMessage>(
                new BatchConfiguration { BatchTriggerTimeout = TimeSpan.FromDays(1), MaxBatchSize = 2 },
                Substitute.For<IBatchHandler<IMessage>>(),
                _consumer,
                _handlersRegistry);

            var message = _fixture.Create<IMessage>();

            observer.Dispose();

            Func<Task> act = () => observer.OnMessageAppeared(
                message, CancellationToken.None);

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

            using var sempahore = new SemaphoreSlim(0);

            var batchHandler = Substitute.For<IBatchHandler<IMessage>>();
            batchHandler.Handle(default, default)
                .ThrowsForAnyArgs(new TestException())
                .AndDoes(_ => sempahore.Release());

            const int batchCount = 2;

            var observer = new BatchMessageObserver<IMessage>(
                new BatchConfiguration { BatchTriggerTimeout = TimeSpan.FromDays(1), MaxBatchSize = batchCount },
                batchHandler,
                _consumer,
                _handlersRegistry);

            var message = _fixture.Create<IMessage>();

            for (var i = 0; i < batchCount; i++)
                await observer.OnMessageAppeared(message, CancellationToken.None);

            await sempahore.WaitAsync();
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

            using var sempahore = new SemaphoreSlim(0);

            var batchHandler = Substitute.For<IBatchHandler<IMessage>>();
            batchHandler.Handle(default, default)
                .ThrowsForAnyArgs(new TestException())
                .AndDoes(_ => sempahore.Release());

            const int batchCount = 2;

            var observer = new BatchMessageObserver<IMessage>(
                new BatchConfiguration { BatchTriggerTimeout = TimeSpan.FromDays(1), MaxBatchSize = batchCount },
                batchHandler,
                _consumer,
                _handlersRegistry);

            var message = _fixture.Create<IMessage>();

            for (var i = 0; i < batchCount; i++)
                await observer.OnMessageAppeared(message, CancellationToken.None);

            await sempahore.WaitAsync();
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

            var observer = new BatchMessageObserver<IMessage>(
                new BatchConfiguration { BatchTriggerTimeout = TimeSpan.FromDays(1), MaxBatchSize = batchCount },
                Substitute.For<IBatchHandler<IMessage>>(),
                _consumer,
                _handlersRegistry);

            var message = _fixture.Create<IMessage>();

            for (var i = 0; i < batchCount; i++)
                await observer.OnMessageAppeared(message, CancellationToken.None);

            observer.Dispose();
            observer.Dispose();
        }

        public sealed class TestException : Exception
        {
        }

        private sealed class TestConsumer : IConsumer<IMessage>
        {
            public readonly List<IMessage> Acks = new();
            public readonly CancellationTokenSource CancellationTokenSource = new();

            public CancellationToken CancellationToken => CancellationTokenSource.Token;

            public string Subscription { get; } = "Some";

            public void Acknowledge(in IMessage message) =>
                Acks.Add(message);

            public void Acknowledge(IReadOnlyList<IMessage> messages) =>
                Acks.AddRange(messages);

            public void Cancel() => CancellationTokenSource.Cancel();
        }
    }
}