using Eventso.Subscription.Observing.DeadLetter;
using Microsoft.Extensions.Logging.Abstractions;

namespace Eventso.Subscription.Tests;

public sealed class PoisonEventHandlerTests
{
    private readonly Fixture _fixture = new();

    private readonly List<TestEvent> _inboxPoisonEvents = new();
    private readonly List<TestEvent> _innerHandlerEvents = new();

    private readonly PoisonEventHandler<TestEvent> _underTestHandler;

    public PoisonEventHandlerTests()
    {
        _underTestHandler = new PoisonEventHandler<TestEvent>(
            "topic",
            CreatePoisonEventInbox(),
            CreteInnerHandler());
    }

    [Fact]
    public async Task SingleNotDeadPredecessorAndNotDeadInScope_HandledSuccessfully()
    {
        var @event = HealthyEvent();

        await _underTestHandler.Handle(@event, default, CancellationToken.None);

        _inboxPoisonEvents.Should().BeEmpty();
        _innerHandlerEvents.Should().ContainSingle().Subject.Should().Be(@event);
    }

    [Fact]
    public async Task BatchNotDeadPredecessorAndNotDeadInScope_HandledSuccessfully()
    {
        var events = Enumerable.Range(0, 9).Select(_ => HealthyEvent()).ToConvertibleCollection();

        await _underTestHandler.Handle(events, default, CancellationToken.None);

        _inboxPoisonEvents.Should().BeEmpty();
        _innerHandlerEvents.Should().BeEquivalentTo(events);
    }

    [Fact]
    public async Task SingleDeadPredecessorAndNotDeadInScope_PutToInboxAndNotHandled()
    {
        var key = _fixture.Create<Guid>();

        var predecessorInInbox = PoisonEvent(key);
        _inboxPoisonEvents.Add(predecessorInInbox);

        var @event = HealthyEvent(key);
        await _underTestHandler.Handle(@event, default, CancellationToken.None);

        _inboxPoisonEvents.Should().BeEquivalentTo(new[] { predecessorInInbox, @event });
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

        var healthyEvents = Enumerable.Range(0, 5).Select(_ => HealthyEvent()).ToArray();
        var toPoisonEvents = new[] { HealthyEvent(key1), HealthyEvent(key2), HealthyEvent(key3), HealthyEvent(key2) };
        var events = healthyEvents.Concat(toPoisonEvents).OrderBy(_ => Guid.NewGuid()).ToConvertibleCollection();

        await _underTestHandler.Handle(events, default, CancellationToken.None);

        _inboxPoisonEvents.Should().BeEquivalentTo(predecessors.Concat(toPoisonEvents));
        _innerHandlerEvents.Should().BeEquivalentTo(healthyEvents);
    }

    [Fact]
    public async Task SingleNotDeadPredecessorAndDeadInScope_PutToInboxAndNotHandled()
    {
        var poisonEvent = PoisonEvent();

        await _underTestHandler.Handle(poisonEvent, default, CancellationToken.None);

        _inboxPoisonEvents.Should().ContainSingle().Subject.Should().Be(poisonEvent);
        _innerHandlerEvents.Should().ContainSingle().Subject.Should().Be(poisonEvent);
    }

    [Fact]
    public async Task BatchNotDeadPredecessorAndDeadInScope_ExceptionIsThrown()
    {
        var poisonEvents = new[] { PoisonEvent(), PoisonEvent(), PoisonEvent() };

        var healthyEvents = Enumerable.Range(0, 6).Select(_ => HealthyEvent()).ToArray();
        var events = healthyEvents.Concat(poisonEvents)
            .OrderBy(_ => Guid.NewGuid())
            .ToConvertibleCollection();

        await FluentActions.Awaiting(() => _underTestHandler.Handle(events, default, CancellationToken.None))
            .Should()
            .ThrowAsync<PoisonTestException>();

        _inboxPoisonEvents.Should().BeEmpty();
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

        var healthyEvents = Enumerable.Range(0, 10).Select(_ => HealthyEvent()).ToArray();
        var predecessorPoisonEvents = new[]
            { HealthyEvent(key1), HealthyEvent(key2), HealthyEvent(key3), HealthyEvent(key2) };
        var events = healthyEvents
            .Concat(predecessorPoisonEvents)
            .Concat(scopePoisonEvents)
            .OrderBy(_ => Guid.NewGuid())
            .ToConvertibleCollection();

        await FluentActions.Awaiting(() => _underTestHandler.Handle(events, default, CancellationToken.None))
            .Should()
            .ThrowAsync<PoisonTestException>();

        _inboxPoisonEvents.Should().BeEquivalentTo(poisonPredecessors.Concat(predecessorPoisonEvents));
        _innerHandlerEvents.Should().BeEquivalentTo(events.Except(predecessorPoisonEvents));
    }

    private IPoisonEventInbox<TestEvent> CreatePoisonEventInbox()
    {
        var poisonEventInbox = Substitute.For<IPoisonEventInbox<TestEvent>>();
        poisonEventInbox.Add(default!, default!, default)
            .ReturnsForAnyArgs(Task.CompletedTask)
            .AndDoes(c => _inboxPoisonEvents.Add(c.Arg<TestEvent>()));
        poisonEventInbox.GetEventKeys(default!, default)
            .ReturnsForAnyArgs(_ => Task.FromResult((IKeySet<TestEvent>)new KeySet(_inboxPoisonEvents)));

        return poisonEventInbox;
    }

    private IEventHandler<TestEvent> CreteInnerHandler()
    {
        var innerHandler = Substitute.For<IEventHandler<TestEvent>>();

        var bh = new BatchHandler<TestEvent>(
            Substitute.For<IEventHandler<TestEvent>>(),
            Substitute.For<IConsumer<TestEvent>>(),
            NullLogger<BatchEventObserver<TestEvent>>.Instance);

        innerHandler.Handle(Arg.Is<TestEvent>(e => !(bool)e.GetMessage()), default, default)
            .Returns(Task.CompletedTask)
            .AndDoes(c => _innerHandlerEvents.Add(c.Arg<TestEvent>()));

        innerHandler.Handle(Arg.Is<TestEvent>(e => (bool)e.GetMessage()), default, default)
            .Throws<PoisonTestException>()
            .AndDoes(c => _innerHandlerEvents.Add(c.Arg<TestEvent>()));
        
        innerHandler.Handle(Arg.Is<IConvertibleCollection<TestEvent>>(e => e.All(ee => !(bool)ee.GetMessage())), default, default)
            .Returns(Task.CompletedTask)
            .AndDoes(c => _innerHandlerEvents.AddRange(c.Arg<IConvertibleCollection<TestEvent>>()));
        
        innerHandler.Handle(Arg.Is<IConvertibleCollection<TestEvent>>(e => e.Any(ee => (bool)ee.GetMessage())), default, default)
            .Throws<PoisonTestException>()
            .AndDoes(c => _innerHandlerEvents.AddRange(c.Arg<IConvertibleCollection<TestEvent>>()));

        return innerHandler;
    }

    private TestEvent PoisonEvent(Guid key = default)
        => new(key != default ? key : _fixture.Create<Guid>(), true);

    private TestEvent HealthyEvent(Guid key = default)
        => new(key != default ? key : _fixture.Create<Guid>(), false);

    private sealed class KeySet : IKeySet<TestEvent>
    {
        private readonly List<TestEvent> _poison;

        public KeySet(List<TestEvent> poison)
            => _poison = poison;

        public bool Contains(in TestEvent item)
        {
            foreach (var r in _poison)
                if (r.Key == item.Key)
                    return true;

            return false;
        }

        public bool IsEmpty()
            => _poison.Count == 0;
    }

    private sealed class PoisonTestException : Exception;
}