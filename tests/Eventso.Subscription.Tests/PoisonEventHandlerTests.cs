using Eventso.Subscription.Observing.DeadLetter;
using Microsoft.Extensions.Logging.Abstractions;

namespace Eventso.Subscription.Tests;

public sealed class PoisonEventHandlerTests
{
    private readonly Fixture _fixture = new();

    private readonly List<TestEvent> _inboxPoisonEvents = new();
    private readonly List<TestEvent> _scopePoisonEvents = new();
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
        _scopePoisonEvents.Should().BeEmpty();
        _innerHandlerEvents.Should().ContainSingle().Subject.Should().Be(@event);
    }

    [Fact]
    public async Task BatchNotDeadPredecessorAndNotDeadInScope_HandledSuccessfully()
    {
        var events = Enumerable.Range(0, 9).Select(_ => HealthyEvent()).ToConvertibleCollection();

        await _underTestHandler.Handle(events, default, CancellationToken.None);

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

        var @event = HealthyEvent(key);
        await _underTestHandler.Handle(@event, default, CancellationToken.None);

        _inboxPoisonEvents.Should().BeEquivalentTo(new[] { predecessorInInbox, @event });
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

        var healthyEvents = Enumerable.Range(0, 5).Select(_ => HealthyEvent()).ToArray();
        var toPoisonEvents = new[] { HealthyEvent(key1), HealthyEvent(key2), HealthyEvent(key3), HealthyEvent(key2) };
        var events = healthyEvents.Concat(toPoisonEvents).OrderBy(_ => Guid.NewGuid()).ToConvertibleCollection();

        await _underTestHandler.Handle(events, default, CancellationToken.None);

        _inboxPoisonEvents.Should().BeEquivalentTo(predecessors.Concat(toPoisonEvents));
        _scopePoisonEvents.Should().BeEmpty();
        _innerHandlerEvents.Should().BeEquivalentTo(healthyEvents);
    }

    [Fact]
    public async Task SingleNotDeadPredecessorAndDeadInScope_PutToInboxAndNotHandled()
    {
        var poisonEvent = PoisonEvent();

        await _underTestHandler.Handle(poisonEvent, default, CancellationToken.None);

        _inboxPoisonEvents.Should().ContainSingle().Subject.Should().Be(poisonEvent);
        _scopePoisonEvents.Should().ContainSingle().Subject.Should().Be(poisonEvent);
        _innerHandlerEvents.Should().ContainSingle().Subject.Should().Be(poisonEvent);
    }

    [Fact]
    public async Task BatchNotDeadPredecessorAndDeadInScope_HandledSuccessfully()
    {
        var poisonEvents = new[] { PoisonEvent(), PoisonEvent(), PoisonEvent() };

        var healthyEvents = Enumerable.Range(0, 6).Select(_ => HealthyEvent()).ToArray();
        var events = healthyEvents.Concat(poisonEvents)
            .OrderBy(_ => Guid.NewGuid())
            .ToConvertibleCollection();

        await _underTestHandler.Handle(events, default, CancellationToken.None);

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

        var healthyEvents = Enumerable.Range(0, 10).Select(_ => HealthyEvent()).ToArray();
        var predecessorPoisonEvents = new[] { HealthyEvent(key1), HealthyEvent(key2), HealthyEvent(key3), HealthyEvent(key2) };
        var events = healthyEvents
            .Concat(predecessorPoisonEvents)
            .Concat(scopePoisonEvents)
            .OrderBy(_ => Guid.NewGuid())
            .ToConvertibleCollection();

        await _underTestHandler.Handle(events, default, CancellationToken.None);

        _inboxPoisonEvents.Should().BeEquivalentTo(
            poisonPredecessors
                .Concat(predecessorPoisonEvents)
                .Concat(scopePoisonEvents));
        _scopePoisonEvents.Should().BeEquivalentTo(scopePoisonEvents);
        _innerHandlerEvents.Should().BeEquivalentTo(healthyEvents.Concat(scopePoisonEvents));
    }

    private IPoisonEventInbox<TestEvent> CreatePoisonEventInbox()
    {
        var poisonEventInbox = Substitute.For<IPoisonEventInbox<TestEvent>>();
        poisonEventInbox.Add(default(TestEvent), default!, default)
            .ReturnsForAnyArgs(Task.CompletedTask)
            .AndDoes(c => _inboxPoisonEvents.Add(c.Arg<TestEvent>()));

        return poisonEventInbox;
    }

    // private IDeadLetterQueueScopeFactory CreateDeadLetterQueueScopeFactory()
    // {
    //     var deadLetterQueueScopeFactory = Substitute.For<IDeadLetterQueueScopeFactory>();
    //     deadLetterQueueScopeFactory.Create(default(TestEvent))
    //         .ReturnsForAnyArgs(_ => CreateScope());
    //     deadLetterQueueScopeFactory.Create(default(IReadOnlyCollection<TestEvent>)!)
    //         .ReturnsForAnyArgs(_ => CreateScope());
    //
    //     return deadLetterQueueScopeFactory;
    //
    //     IDeadLetterQueueScope<TestEvent> CreateScope()
    //     {
    //         var scope = Substitute.For<IDeadLetterQueueScope<TestEvent>>();
    //         scope.GetPoisonEvents().ReturnsForAnyArgs(_scopePoisonEvents);
    //         return scope;
    //     }
    // }

    private IEventHandler<TestEvent> CreteInnerHandler()
    {
        var innerHandler = Substitute.For<IEventHandler<TestEvent>>();

        innerHandler.Handle(Arg.Is<TestEvent>(e => !(bool)e.GetMessage()), default, default)
            .Returns(Task.CompletedTask)
            .AndDoes(c => _innerHandlerEvents.Add(c.Arg<TestEvent>()));

        innerHandler.Handle(Arg.Is<TestEvent>(e => (bool)e.GetMessage()), default, default)
            .Throws<Exception>()
            .AndDoes(c => _innerHandlerEvents.Add(c.Arg<TestEvent>()))
            .AndDoes(c => _scopePoisonEvents.Add(c.Arg<TestEvent>()));
        
        innerHandler.Handle(Arg.Is<IConvertibleCollection<TestEvent>>(e => e.All(ee => !(bool)ee.GetMessage())), default, default)
            .Returns(Task.CompletedTask)
            .AndDoes(c => _innerHandlerEvents.AddRange(c.Arg<IConvertibleCollection<TestEvent>>()));
        
        innerHandler.Handle(Arg.Is<IConvertibleCollection<TestEvent>>(e => e.Any(ee => (bool)ee.GetMessage())), default, default)
            .Throws<Exception>()
            .AndDoes(c =>
            {
                var events = c.Arg<IConvertibleCollection<TestEvent>>();
                if (events.Count > 1)
                    return;
                _innerHandlerEvents.AddRange(events);
                _scopePoisonEvents.AddRange(events);
            });

        return innerHandler;
    }

    private TestEvent PoisonEvent(Guid key = default)
        => new(key != default ? key : _fixture.Create<Guid>(), true);

    private TestEvent HealthyEvent(Guid key = default)
        => new(key != default ? key : _fixture.Create<Guid>(), false);

    // private sealed class PoisonStreamCollection : IPoisonStreamCollection<TestEvent>
    // {
    //     private readonly List<PoisonEvent<TestEvent>> _poisonEvents;
    //
    //     public PoisonStreamCollection(List<PoisonEvent<TestEvent>> poisonEvents)
    //         => _poisonEvents = poisonEvents;
    //
    //     public bool IsPartOfPoisonStream(TestEvent @event)
    //         => _poisonEvents.Any(ee => @event.Key == ee.Event.Key);
    // }
}