namespace Eventso.Subscription.Tests;

public sealed class EventObserverTests
{
    private readonly Fixture _fixture;
    private readonly TestConsumer _consumer;
    private readonly List<TestEvent> _handledEvents = new();
    private readonly IMessageHandlersRegistry _handlersRegistry;
    private readonly IEventHandler<TestEvent> _eventHandler;

    public EventObserverTests()
    {
        _fixture = new Fixture();
        _fixture.Customize(new AutoNSubstituteCustomization { ConfigureMembers = true });

        _handlersRegistry = _fixture.Create<IMessageHandlersRegistry>();
        _handlersRegistry.ContainsHandlersFor(default!, out Arg.Any<HandlerKind>())
            .ReturnsForAnyArgs(ci =>
            {
                ci[1] = HandlerKind.Single;
                return true;
            });

        var pipelineAction = Substitute.For<IMessagePipelineAction>();
        pipelineAction
            .Invoke<TestEvent>(default!, default, default)
            .ReturnsForAnyArgs(Task.CompletedTask)
            .AndDoes(c => _handledEvents.Add(c.Arg<TestEvent>()));

        _eventHandler = new Observing.EventHandler<TestEvent>(_handlersRegistry, pipelineAction);

        _consumer = new TestConsumer();
    }

    [Fact]
    public async Task ObservingEvents_AllAcknowledged()
    {
        _fixture.Inject(DeserializationStatus.Success);

        var observer = new EventObserver<TestEvent>(
            _eventHandler,
            _consumer,
            _handlersRegistry,
            true);

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
            _eventHandler,
            _consumer,
            _handlersRegistry,
            true);

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
            _eventHandler,
            _consumer,
            _handlersRegistry,
            true);

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
        public DateTime GetUtcTimestamp() => DateTime.UtcNow;

        public IReadOnlyCollection<KeyValuePair<string, object>> GetMetadata()
            => Array.Empty<KeyValuePair<string, object>>();
    }
}