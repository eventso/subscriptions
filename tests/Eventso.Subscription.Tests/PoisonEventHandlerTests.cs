using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AutoFixture;
using Eventso.Subscription.Observing.DeadLetter;
using FluentAssertions;
using NSubstitute;
using Xunit;

namespace Eventso.Subscription.Tests
{
    public sealed class PoisonEventHandlerTests
    {
        // copied from PoisonEventHandler (tests will break on change - be aware)
        private const string PredecessorParkedReason = "Predecessor of event is poison and parked.";
        
        private readonly Fixture _fixture = new();

        private readonly List<PoisonEvent<TestEvent>> _inboxPoisonEvents = new();
        private readonly List<PoisonEvent<TestEvent>> _scopePoisonEvents = new();
        private readonly List<TestEvent> _innerHandlerEvents = new();

        private readonly PoisonEventHandler<TestEvent> _underTestHandler;

        public PoisonEventHandlerTests()
        {
            _underTestHandler = new PoisonEventHandler<TestEvent>(
                CreatePoisonEventInbox(),
                CreateDeadLetterQueueScopeFactory(),
                CreteInnerHandler());
        }

        [Fact]
        public async Task SingleNotDeadPredecessorAndNotDeadInScope_HandledSuccessfully()
        {
            var @event = TestEvent();

            await _underTestHandler.Handle(@event, CancellationToken.None);

            _inboxPoisonEvents.Should().BeEmpty();
            _scopePoisonEvents.Should().BeEmpty();
            _innerHandlerEvents.Should().ContainSingle().Subject.Should().Be(@event);
        }

        [Fact]
        public async Task BatchNotDeadPredecessorAndNotDeadInScope_HandledSuccessfully()
        {
            var events = Enumerable.Range(0, 9).Select(_ => TestEvent()).ToConvertibleCollection();

            await _underTestHandler.Handle(events, CancellationToken.None);

            _inboxPoisonEvents.Should().BeEmpty();
            _scopePoisonEvents.Should().BeEmpty();
            _innerHandlerEvents.Should().BeEquivalentTo(events);
        }

        [Fact]
        public async Task SingleDeadPredecessorAndNotDeadInScope_PutToInboxAndNotHandled()
        {
            var key = _fixture.Create<Guid>();

            var predecessorInInbox = PoisonEvent(key);
            _inboxPoisonEvents.Add(predecessorInInbox);

            var @event = TestEvent(key);
            await _underTestHandler.Handle(@event, CancellationToken.None);

            _inboxPoisonEvents.Should().BeEquivalentTo(
                new[] { predecessorInInbox, PoisonEvent(@event, PredecessorParkedReason) });
            _scopePoisonEvents.Should().BeEmpty();
            _innerHandlerEvents.Should().BeEmpty();
        }

        [Fact]
        public async Task BatchDeadPredecessorAndNotDeadInScope_HandledSuccessfully()
        {
            var key1 = _fixture.Create<Guid>();
            var key2 = _fixture.Create<Guid>();
            var key3 = _fixture.Create<Guid>();

            var predecessors = new[] { PoisonEvent(key1), PoisonEvent(key2), PoisonEvent(key3), PoisonEvent(key1) };
            _inboxPoisonEvents.AddRange(predecessors);

            var healthyEvents = Enumerable.Range(0, 5).Select(_ => TestEvent()).ToArray();
            var toPoisonEvents = new[] { TestEvent(key1), TestEvent(key2), TestEvent(key3), TestEvent(key2) };
            var events = healthyEvents.Concat(toPoisonEvents).OrderBy(_ => Guid.NewGuid()).ToConvertibleCollection();

            await _underTestHandler.Handle(events, CancellationToken.None);

            _inboxPoisonEvents.Should().BeEquivalentTo(
                predecessors.Concat(toPoisonEvents.Select(e => PoisonEvent(e, PredecessorParkedReason))));
            _scopePoisonEvents.Should().BeEmpty();
            _innerHandlerEvents.Should().BeEquivalentTo(healthyEvents);
        }

        [Fact]
        public async Task SingleNotDeadPredecessorAndDeadInScope_PutToInboxAndNotHandled()
        {
            var poisonEvent = PoisonEvent();
            _scopePoisonEvents.Add(poisonEvent);

            await _underTestHandler.Handle(poisonEvent.Event, CancellationToken.None);

            _inboxPoisonEvents.Should().ContainSingle().Subject.Should().Be(poisonEvent);
            _scopePoisonEvents.Should().ContainSingle().Subject.Should().Be(poisonEvent);
            _innerHandlerEvents.Should().ContainSingle().Subject.Should().Be(poisonEvent.Event);
        }

        [Fact]
        public async Task BatchNotDeadPredecessorAndDeadInScope_HandledSuccessfully()
        {
            var poisonEvents = new[] { PoisonEvent(), PoisonEvent(), PoisonEvent() };
            _scopePoisonEvents.AddRange(poisonEvents);

            var healthyEvents = Enumerable.Range(0, 6).Select(_ => TestEvent()).ToArray();
            var events = healthyEvents.Concat(poisonEvents.Select(e => e.Event))
                .OrderBy(_ => Guid.NewGuid())
                .ToConvertibleCollection();

            await _underTestHandler.Handle(events, CancellationToken.None);

            _inboxPoisonEvents.Should().BeEquivalentTo(poisonEvents);
            _scopePoisonEvents.Should().BeEquivalentTo(poisonEvents);
            _innerHandlerEvents.Should().BeEquivalentTo(events);
        }

        [Fact]
        public async Task BatchDeadPredecessorAndDeadInScope_HandledSuccessfully()
        {
            var key1 = _fixture.Create<Guid>();
            var key2 = _fixture.Create<Guid>();
            var key3 = _fixture.Create<Guid>();

            var poisonPredecessors = new[] { PoisonEvent(key1), PoisonEvent(key2), PoisonEvent(key3), PoisonEvent(key1) };
            _inboxPoisonEvents.AddRange(poisonPredecessors);

            var scopePoisonEvents = new[] { PoisonEvent(), PoisonEvent(), PoisonEvent() };
            _scopePoisonEvents.AddRange(scopePoisonEvents);

            var healthyEvents = Enumerable.Range(0, 10).Select(_ => TestEvent()).ToArray();
            var predecessorPoisonEvents = new[] { TestEvent(key1), TestEvent(key2), TestEvent(key3), TestEvent(key2) };
            var events = healthyEvents
                .Concat(predecessorPoisonEvents)
                .Concat(scopePoisonEvents.Select(p => p.Event))
                .OrderBy(_ => Guid.NewGuid())
                .ToConvertibleCollection();

            await _underTestHandler.Handle(events, CancellationToken.None);

            _inboxPoisonEvents.Should().BeEquivalentTo(
                poisonPredecessors
                    .Concat(predecessorPoisonEvents.Select(e => PoisonEvent(e, PredecessorParkedReason)))
                    .Concat(scopePoisonEvents));
            _scopePoisonEvents.Should().BeEquivalentTo(scopePoisonEvents);
            _innerHandlerEvents.Should().BeEquivalentTo(healthyEvents.Concat(scopePoisonEvents.Select(p => p.Event)));
        }

        private IPoisonEventInbox<TestEvent> CreatePoisonEventInbox()
        {
            var poisonEventInbox = Substitute.For<IPoisonEventInbox<TestEvent>>();
            poisonEventInbox.Add(default, default)
                .ReturnsForAnyArgs(Task.CompletedTask)
                .AndDoes(c => _inboxPoisonEvents.AddRange(c.Arg<IReadOnlyCollection<PoisonEvent<TestEvent>>>()));
            poisonEventInbox.IsStreamPoisoned(default, default)
                .ReturnsForAnyArgs(c => Task.FromResult(_inboxPoisonEvents.Any(e => e.Event.Key == c.Arg<TestEvent>().Key)));
            poisonEventInbox.GetPoisonStreamsEvents(default, default)
                .ReturnsForAnyArgs(c =>
                    Task.FromResult<IReadOnlySet<TestEvent>>(c.Arg<IReadOnlyCollection<TestEvent>>()
                        .Where(e => _inboxPoisonEvents.Any(ee => e.Key == ee.Event.Key))
                        .ToHashSet()));

            return poisonEventInbox;
        }

        private IDeadLetterQueueScopeFactory CreateDeadLetterQueueScopeFactory()
        {
            var deadLetterQueueScopeFactory = Substitute.For<IDeadLetterQueueScopeFactory>();
            deadLetterQueueScopeFactory.Create(default(TestEvent))
                .ReturnsForAnyArgs(_ => CreateScope());
            deadLetterQueueScopeFactory.Create(default(IReadOnlyCollection<TestEvent>))
                .ReturnsForAnyArgs(_ => CreateScope());

            return deadLetterQueueScopeFactory;

            IDeadLetterQueueScope<TestEvent> CreateScope()
            {
                var scope = Substitute.For<IDeadLetterQueueScope<TestEvent>>();
                scope.GetPoisonEvents().ReturnsForAnyArgs(_scopePoisonEvents);
                return scope;
            }
        }

        private IEventHandler<TestEvent> CreteInnerHandler()
        {
            var innerHandler = Substitute.For<IEventHandler<TestEvent>>();
            innerHandler.Handle(default(TestEvent), default)
                .ReturnsForAnyArgs(Task.CompletedTask)
                .AndDoes(c => _innerHandlerEvents.Add(c.Arg<TestEvent>()));
            innerHandler.Handle(default(IConvertibleCollection<TestEvent>), default)
                .ReturnsForAnyArgs(Task.CompletedTask)
                .AndDoes(c => _innerHandlerEvents.AddRange(c.Arg<IConvertibleCollection<TestEvent>>()));

            return innerHandler;
        }

        private TestEvent TestEvent(Guid key = default)
            => new(key != default ? key : _fixture.Create<Guid>(), _fixture.Create<RedMessage>());

        private PoisonEvent<TestEvent> PoisonEvent(Guid key = default, string reason = null)
            => new(TestEvent(key), reason ?? _fixture.Create<string>());

        private PoisonEvent<TestEvent> PoisonEvent(TestEvent @event, string reason = null)
            => new(@event, reason ?? _fixture.Create<string>());
    }
}