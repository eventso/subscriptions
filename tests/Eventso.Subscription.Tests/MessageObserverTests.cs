using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AutoFixture;
using AutoFixture.AutoNSubstitute;
using Eventso.Subscription.Configurations;
using Eventso.Subscription.Observing;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Xunit;

namespace Eventso.Subscription.Tests
{
    public sealed class MessageObserverTests
    {
        private readonly Fixture _fixture;
        private readonly TestConsumer _consumer;
        private readonly List<TestMessage> _handledMessages = new();
        private readonly IMessageHandlersRegistry _handlersRegistry;
        private readonly IMessagePipelineAction _pipelineAction;

        public MessageObserverTests()
        {
            _fixture = new Fixture();
            _fixture.Customize(new AutoNSubstituteCustomization { ConfigureMembers = true });

            _handlersRegistry = _fixture.Create<IMessageHandlersRegistry>();
            _handlersRegistry.ContainsHandlersFor(default, out Arg.Any<HandlerKind>())
                .ReturnsForAnyArgs(ci =>
                {
                    ci[1] = HandlerKind.Single;
                    return true;
                });

            _pipelineAction = Substitute.For<IMessagePipelineAction>();
            _pipelineAction
                .Invoke<TestMessage>(default, default)
                .ReturnsForAnyArgs(Task.CompletedTask)
                .AndDoes(c => _handledMessages.Add(c.Arg<TestMessage>()));

            _consumer = new TestConsumer();
        }

        [Fact]
        public async Task ObservingMessages_AllAcknowledged()
        {
            _fixture.Inject(DeserializationStatus.Success);

            var observer = new MessageObserver<TestMessage>(
                _pipelineAction,
                _consumer,
                _handlersRegistry,
                true,
                DeferredAckConfiguration.Disabled,
                NullLogger<MessageObserver<TestMessage>>.Instance);

            var messages = _fixture.CreateMany<TestMessage>(56).ToArray();

            foreach (var message in messages)
                await observer.OnMessageAppeared(message, CancellationToken.None);

            _consumer.Acks.Should()
                .BeEquivalentTo(
                    messages,
                    c => c.WithStrictOrdering());

            _handledMessages.Should().HaveSameCount(messages);
        }

        [Fact]
        public async Task ObservingSkippedByDeserializerMessages_AllAcknowledged()
        {
            _fixture.Inject(DeserializationStatus.Skipped);

            var observer = new MessageObserver<TestMessage>(
                _pipelineAction,
                _consumer,
                _handlersRegistry,
                true,
                DeferredAckConfiguration.Disabled,
                NullLogger<MessageObserver<TestMessage>>.Instance);

            var messages = _fixture.CreateMany<TestMessage>(56).ToArray();

            foreach (var message in messages)
                await observer.OnMessageAppeared(message, CancellationToken.None);

            _consumer.Acks.Should()
                .BeEquivalentTo(
                    messages,
                    c => c.WithStrictOrdering());

            _handledMessages.Should().BeEmpty();
        }

        [Fact]
        public async Task ObservingMixedMessages_AllAcknowledged()
        {
            var skippedMessages = Enumerable.Repeat(0, 25)
                .Select(_ =>
                {
                    var msg = Substitute.For<TestMessage>();
                    msg.DeserializationResult.Returns(DeserializationStatus.Skipped);
                    return msg;
                }).ToArray();

            var payloadMessages = Enumerable.Repeat(0, 25)
                .Select(_ =>
                {
                    var msg = Substitute.For<TestMessage>();
                    msg.DeserializationResult.Returns(DeserializationStatus.Success);

                    return msg;
                }).ToArray();

            var observer = new MessageObserver<TestMessage>(
                _pipelineAction,
                _consumer,
                _handlersRegistry,
                true,
                DeferredAckConfiguration.Disabled,
                NullLogger<MessageObserver<TestMessage>>.Instance);

            var messages = skippedMessages.Concat(payloadMessages)
                .OrderBy(_ => Guid.NewGuid())
                .ToArray();

            foreach (var message in messages)
                await observer.OnMessageAppeared(message, CancellationToken.None);

            _consumer.Acks.Should()
                .BeEquivalentTo(
                    messages,
                    c => c.WithStrictOrdering());

            _handledMessages.Should()
                .BeEquivalentTo(
                    payloadMessages,
                    c => c.WithStrictOrdering());
        }

        [Fact]
        public async Task ObservingSkipped_DeferredAckTimeout_AllAcknowledged()
        {
            _fixture.Inject(DeserializationStatus.Skipped);

            const int batchTimeoutMs = 300;
            const int eventsCount = 56;

            var observer = new MessageObserver<TestMessage>(
                _pipelineAction,
                _consumer,
                _handlersRegistry,
                true,
                new DeferredAckConfiguration
                {
                    Timeout = TimeSpan.FromMilliseconds(batchTimeoutMs),
                    MaxBufferSize = eventsCount * 2
                },
                NullLogger<MessageObserver<TestMessage>>.Instance);

            var messages = _fixture.CreateMany<TestMessage>(eventsCount).ToArray();

            foreach (var message in messages)
                await observer.OnMessageAppeared(message, CancellationToken.None);

            await Task.Delay(batchTimeoutMs + 50);

            await observer.OnMessageTimeout(CancellationToken.None);

            _consumer.Acks.Should()
                .BeEquivalentTo(
                    messages,
                    c => c.WithStrictOrdering());

            _handledMessages.Should().BeEmpty();
        }

        [Fact]
        public async Task ObservingSkipped_DeferredAckBufferExceeded_AllAcknowledged()
        {
            _fixture.Inject(DeserializationStatus.Skipped);

            const int eventsCount = 56;

            var observer = new MessageObserver<TestMessage>(
                _pipelineAction,
                _consumer,
                _handlersRegistry,
                true,
                new DeferredAckConfiguration
                {
                    Timeout = Timeout.InfiniteTimeSpan,
                    MaxBufferSize = eventsCount - 10
                },
                NullLogger<MessageObserver<TestMessage>>.Instance);

            var messages = _fixture.CreateMany<TestMessage>(eventsCount).ToArray();

            foreach (var message in messages)
                await observer.OnMessageAppeared(message, CancellationToken.None);

            await observer.OnMessageTimeout(CancellationToken.None);

            _consumer.Acks.Should()
                .BeEquivalentTo(
                    messages.SkipLast(10),
                    c => c.WithStrictOrdering());

            _handledMessages.Should().BeEmpty();
        }

        private sealed class TestConsumer : IConsumer<TestMessage>
        {
            public readonly List<TestMessage> Acks = new();
            public readonly CancellationTokenSource CancellationTokenSource = new();

            public CancellationToken CancellationToken => CancellationTokenSource.Token;

            public string Subscription { get; } = "Some";

            public void Acknowledge(in TestMessage message) =>
                Acks.Add(message);

            public void Acknowledge(IReadOnlyList<TestMessage> messages) =>
                Acks.AddRange(messages);

            public void Cancel() => CancellationTokenSource.Cancel();
        }

        public class TestMessage : IMessage
        {
            public virtual DeserializationStatus DeserializationResult { get; set; }
            public Guid GetKey() => throw new NotImplementedException();
            public object GetPayload() => this;
            public string GetIdentity() => throw new NotImplementedException();

            public IReadOnlyCollection<KeyValuePair<string, object>> GetMetadata()
                => Array.Empty<KeyValuePair<string, object>>();
        }
    }
}