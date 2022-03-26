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
    public sealed class EventObserverTests
    {
        private readonly Fixture _fixture;
        private readonly TestConsumer _consumer;
        private readonly List<TestEvent> _handledEvents = new();
        private readonly IMessageHandlersRegistry _handlersRegistry;
        private readonly IMessagePipelineAction _pipelineAction;

        public EventObserverTests()
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
                .Invoke<TestEvent>(default, default)
                .ReturnsForAnyArgs(Task.CompletedTask)
                .AndDoes(c => _handledEvents.Add(c.Arg<TestEvent>()));

            _consumer = new TestConsumer();
        }

        [Fact]
        public async Task ObservingEvents_AllAcknowledged()
        {
            _fixture.Inject(DeserializationStatus.Success);

            var observer = new EventObserver<TestEvent>(
                _pipelineAction,
                _consumer,
                _handlersRegistry,
                true,
                DeferredAckConfiguration.Disabled,
                NullLogger<EventObserver<TestEvent>>.Instance);

            var events = _fixture.CreateMany<TestEvent>(56).ToArray();

            foreach (var @event in events)
                await observer.OnEventAppeared(@event, CancellationToken.None);

            _consumer.Acks.Should()
                .BeEquivalentTo(
                    events,
                    c => c.WithStrictOrdering());

            _handledEvents.Should().HaveSameCount(events);
        }

        [Fact]
        public async Task ObservingSkippedByDeserializerEvents_AllAcknowledged()
        {
            _fixture.Inject(DeserializationStatus.Skipped);

            var observer = new EventObserver<TestEvent>(
                _pipelineAction,
                _consumer,
                _handlersRegistry,
                true,
                DeferredAckConfiguration.Disabled,
                NullLogger<EventObserver<TestEvent>>.Instance);

            var events = _fixture.CreateMany<TestEvent>(56).ToArray();

            foreach (var @event in events)
                await observer.OnEventAppeared(@event, CancellationToken.None);

            _consumer.Acks.Should()
                .BeEquivalentTo(
                    events,
                    c => c.WithStrictOrdering());

            _handledEvents.Should().BeEmpty();
        }

        [Fact]
        public async Task ObservingMixedEvents_AllAcknowledged()
        {
            var skippedEvents = Enumerable.Repeat(0, 25)
                .Select(_ =>
                {
                    var msg = Substitute.For<TestEvent>();
                    msg.DeserializationResult.Returns(DeserializationStatus.Skipped);
                    return msg;
                }).ToArray();

            var successEvents = Enumerable.Repeat(0, 25)
                .Select(_ =>
                {
                    var @event = Substitute.For<TestEvent>();
                    @event.DeserializationResult.Returns(DeserializationStatus.Success);

                    return @event;
                }).ToArray();

            var observer = new EventObserver<TestEvent>(
                _pipelineAction,
                _consumer,
                _handlersRegistry,
                true,
                DeferredAckConfiguration.Disabled,
                NullLogger<EventObserver<TestEvent>>.Instance);

            var events = skippedEvents.Concat(successEvents)
                .OrderBy(_ => Guid.NewGuid())
                .ToArray();

            foreach (var @event in events)
                await observer.OnEventAppeared(@event, CancellationToken.None);

            _consumer.Acks.Should()
                .BeEquivalentTo(
                    events,
                    c => c.WithStrictOrdering());

            _handledEvents.Should()
                .BeEquivalentTo(
                    successEvents,
                    c => c.WithStrictOrdering());
        }

        [Fact]
        public async Task ObservingSkipped_DeferredAckTimeout_AllAcknowledged()
        {
            _fixture.Inject(DeserializationStatus.Skipped);

            const int batchTimeoutMs = 300;
            const int eventsCount = 56;

            var observer = new EventObserver<TestEvent>(
                _pipelineAction,
                _consumer,
                _handlersRegistry,
                true,
                new DeferredAckConfiguration
                {
                    Timeout = TimeSpan.FromMilliseconds(batchTimeoutMs),
                    MaxBufferSize = eventsCount * 2
                },
                NullLogger<EventObserver<TestEvent>>.Instance);

            var events = _fixture.CreateMany<TestEvent>(eventsCount).ToArray();

            foreach (var @event in events)
                await observer.OnEventAppeared(@event, CancellationToken.None);

            await Task.Delay(batchTimeoutMs + 50);

            await observer.OnEventTimeout(CancellationToken.None);

            _consumer.Acks.Should()
                .BeEquivalentTo(
                    events,
                    c => c.WithStrictOrdering());

            _handledEvents.Should().BeEmpty();
        }

        [Fact]
        public async Task ObservingSkipped_DeferredAckBufferExceeded_AllAcknowledged()
        {
            _fixture.Inject(DeserializationStatus.Skipped);

            const int eventsCount = 56;

            var observer = new EventObserver<TestEvent>(
                _pipelineAction,
                _consumer,
                _handlersRegistry,
                true,
                new DeferredAckConfiguration
                {
                    Timeout = Timeout.InfiniteTimeSpan,
                    MaxBufferSize = eventsCount - 10
                },
                NullLogger<EventObserver<TestEvent>>.Instance);

            var events = _fixture.CreateMany<TestEvent>(eventsCount).ToArray();

            foreach (var @event in events)
                await observer.OnEventAppeared(@event, CancellationToken.None);

            await observer.OnEventTimeout(CancellationToken.None);

            _consumer.Acks.Should()
                .BeEquivalentTo(
                    events.SkipLast(10),
                    c => c.WithStrictOrdering());

            _handledEvents.Should().BeEmpty();
        }

        private sealed class TestConsumer : IConsumer<TestEvent>
        {
            public readonly List<TestEvent> Acks = new();
            public readonly CancellationTokenSource CancellationTokenSource = new();

            public CancellationToken CancellationToken => CancellationTokenSource.Token;

            public string Subscription { get; } = "Some";

            public void Acknowledge(in TestEvent events) =>
                Acks.Add(events);

            public void Acknowledge(IReadOnlyList<TestEvent> events) =>
                Acks.AddRange(events);

            public void Cancel() => CancellationTokenSource.Cancel();
        }

        public class TestEvent : IEvent
        {
            public virtual DeserializationStatus DeserializationResult { get; set; }
            public Guid GetKey() => throw new NotImplementedException();
            public object GetMessage() => this;
            public string GetIdentity() => throw new NotImplementedException();

            public IReadOnlyCollection<KeyValuePair<string, object>> GetMetadata()
                => Array.Empty<KeyValuePair<string, object>>();
        }
    }
}