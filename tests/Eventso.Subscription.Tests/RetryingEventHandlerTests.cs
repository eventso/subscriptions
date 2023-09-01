using Confluent.Kafka;
using Eventso.Subscription.Kafka;
using Eventso.Subscription.Kafka.DeadLetter;
using Eventso.Subscription.Kafka.DeadLetter.Store;
using Eventso.Subscription.Observing.DeadLetter;

namespace Eventso.Subscription.Tests;

public sealed class RetryingEventHandlerTests
{
    private readonly Fixture _fixture = new();

    private readonly List<Event> _innerHandlerEvents = new();
    private readonly List<PoisonEvent<Event>> _scopePoisonEvents = new();
    private readonly List<TopicPartitionOffset> _removedOffsets = new();
    private readonly List<OccuredFailure> _storedFailures = new();

    private readonly RetryingEventHandler _underTestHandler;

    public RetryingEventHandlerTests()
    {
        _underTestHandler = new RetryingEventHandler(
            CreteInnerHandler(),
            CreateDeadLetterQueueScopeFactory(),
            CreatePoisonEventStore());
    }

    [Fact]
    public async Task SingleHealed_RemovedFromStore()
    {
        var @event = Event();

        await _underTestHandler.Handle(@event, CancellationToken.None);

        _innerHandlerEvents.Should().ContainSingle().Subject.Should().Be(@event);
        _removedOffsets.Should().ContainSingle().Subject.Should().Be(@event.GetTopicPartitionOffset());
        _storedFailures.Should().BeEmpty();
    }

    [Fact]
    public async Task BatchHealed_RemovedFromStore()
    {
        var events = Enumerable.Range(0, 9).Select(_ => Event()).ToConvertibleCollection();

        await _underTestHandler.Handle(events, CancellationToken.None);

        _innerHandlerEvents.Should().BeEquivalentTo(events);
        _removedOffsets.Should().BeEquivalentTo(events.Select(e => e.GetTopicPartitionOffset()));
        _storedFailures.Should().BeEmpty();
    }

    [Fact]
    public async Task SingleStillPoison_FailuresAdded()
    {
        var poisonEvent = PoisonEvent();
        _scopePoisonEvents.Add(poisonEvent);

        await _underTestHandler.Handle(poisonEvent.Event, CancellationToken.None);

        _innerHandlerEvents.Should().ContainSingle().Subject.Should().Be(poisonEvent.Event);
        _removedOffsets.Should().BeEmpty();
        _storedFailures.Should().ContainSingle().Subject.Should().Be(
            new OccuredFailure(poisonEvent.Event.GetTopicPartitionOffset(), poisonEvent.Reason));
    }

    [Fact]
    public async Task BatchStillPoison_FailuresAdded()
    {
        var poisonEvents = new[] { PoisonEvent(), PoisonEvent(), PoisonEvent() };
        _scopePoisonEvents.AddRange(poisonEvents);
            
        var events = poisonEvents.Select(e => e.Event).ToConvertibleCollection();

        await _underTestHandler.Handle(events, CancellationToken.None);

        _innerHandlerEvents.Should().BeEquivalentTo(events);
        _removedOffsets.Should().BeEmpty();
        _storedFailures.Should().BeEquivalentTo(
            poisonEvents.Select(e => new OccuredFailure(e.Event.GetTopicPartitionOffset(), e.Reason)));
    }

    [Fact]
    public async Task BatchPartiallyPoisonAndHealed_FailuresAddedAndHealedRemoved()
    {
        var poisonEvents = new[] { PoisonEvent(), PoisonEvent(), PoisonEvent() };
        _scopePoisonEvents.AddRange(poisonEvents);
            
        var healthyEvents = Enumerable.Range(0, 6).Select(_ => Event()).ToArray();
        var events = healthyEvents.Concat(poisonEvents.Select(e => e.Event))
            .OrderBy(_ => Guid.NewGuid())
            .ToConvertibleCollection();

        await _underTestHandler.Handle(events, CancellationToken.None);

        _innerHandlerEvents.Should().BeEquivalentTo(events);
        _removedOffsets.Should().BeEquivalentTo(healthyEvents.Select(e => e.GetTopicPartitionOffset()));
        _storedFailures.Should().BeEquivalentTo(
            poisonEvents.Select(e => new OccuredFailure(e.Event.GetTopicPartitionOffset(), e.Reason)));
    }

    private IPoisonEventStore CreatePoisonEventStore()
    {
        var poisonEventStore = Substitute.For<IPoisonEventStore>();
        poisonEventStore.Remove(default(TopicPartitionOffset)!, default)
            .ReturnsForAnyArgs(Task.CompletedTask)
            .AndDoes(c => _removedOffsets.Add(c.Arg<TopicPartitionOffset>()));
        poisonEventStore.Remove(default(IReadOnlyCollection<TopicPartitionOffset>)!, default)
            .ReturnsForAnyArgs(Task.CompletedTask)
            .AndDoes(c => _removedOffsets.AddRange(c.Arg<IReadOnlyCollection<TopicPartitionOffset>>()));
        poisonEventStore.AddFailure(default, default, default)
            .ReturnsForAnyArgs(Task.CompletedTask)
            .AndDoes(c => _storedFailures.Add(c.Arg<OccuredFailure>()));
        poisonEventStore.AddFailures(default, default!, default)
            .ReturnsForAnyArgs(Task.CompletedTask)
            .AndDoes(c => _storedFailures.AddRange(c.Arg<IReadOnlyCollection<OccuredFailure>>()));

        return poisonEventStore;
    }

    private IDeadLetterQueueScopeFactory CreateDeadLetterQueueScopeFactory()
    {
        var deadLetterQueueScopeFactory = Substitute.For<IDeadLetterQueueScopeFactory>();
        deadLetterQueueScopeFactory.Create(default(Event))
            .ReturnsForAnyArgs(_ => CreateScope());
        deadLetterQueueScopeFactory.Create(default(IReadOnlyCollection<Event>)!)
            .ReturnsForAnyArgs(_ => CreateScope());

        return deadLetterQueueScopeFactory;

        IDeadLetterQueueScope<Event> CreateScope()
        {
            var scope = Substitute.For<IDeadLetterQueueScope<Event>>();
            scope.GetPoisonEvents().ReturnsForAnyArgs(_scopePoisonEvents);
            return scope;
        }
    }

    private IEventHandler<Event> CreteInnerHandler()
    {
        var innerHandler = Substitute.For<IEventHandler<Event>>();
        innerHandler.Handle(default(Event), default)
            .ReturnsForAnyArgs(Task.CompletedTask)
            .AndDoes(c => _innerHandlerEvents.Add(c.Arg<Event>()));
        innerHandler.Handle(default(IConvertibleCollection<Event>)!, default)
            .ReturnsForAnyArgs(Task.CompletedTask)
            .AndDoes(c => _innerHandlerEvents.AddRange(c.Arg<IConvertibleCollection<Event>>()));

        return innerHandler;
    }

    private Event Event()
        => new(_fixture.Create<ConsumeResult<Guid, ConsumedMessage>>());

    private PoisonEvent<Event> PoisonEvent()
        => new(Event(), _fixture.Create<string>());
}