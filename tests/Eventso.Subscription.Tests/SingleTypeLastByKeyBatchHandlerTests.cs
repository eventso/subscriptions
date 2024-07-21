namespace Eventso.Subscription.Tests;

public sealed class SingleTypeLastByKeyEventHandlerTests
{
    private readonly List<object> _handledEvents = new();
    private readonly List<IReadOnlyCollection<object>> _handledBatches = new();
    private readonly SingleTypeLastByKeyEventHandler<TestEvent> _handler;
    private readonly Fixture _fixture = new();

    public SingleTypeLastByKeyEventHandlerTests()
    {
        var registry = Substitute.For<IMessageHandlersRegistry>();
        registry
            .ContainsHandlersFor(Arg.Any<Type>(), out Arg.Any<HandlerKind>())
            .Returns(x => { 
                x[1] = HandlerKind.Batch;
                return true;
            });

        var action = Substitute.For<IMessagePipelineAction>();
        action.Invoke(default(IReadOnlyCollection<RedMessage>)!, default, default)
            .ReturnsForAnyArgs(Task.CompletedTask)
            .AndDoes(c =>
            {
                _handledEvents.AddRange(c.Arg<IReadOnlyCollection<RedMessage>>());
                _handledBatches.Add(c.Arg<IReadOnlyCollection<RedMessage>>());
            });

        var eventHandler = new Observing.EventHandler<TestEvent>(registry, action);
        _handler = new SingleTypeLastByKeyEventHandler<TestEvent>(eventHandler);
    }

    [Fact]
    public async Task HandlingMessages_LastByKeyHandled()
    {
        var keys = _fixture.CreateMany<Guid>(3).ToArray();

        var events = keys.SelectMany(k => _fixture.CreateMany<RedMessage>(10)
                .Select(e => new TestEvent(k, e)))
            .OrderBy(_ => Guid.NewGuid())
            .ToConvertibleCollection();

        await _handler.Handle(events, default, CancellationToken.None);

        _handledEvents.Should().BeEquivalentTo(
            events
                .GroupBy(x => x.GetKey())
                .Select(x => x.Last().GetMessage()));

        _handledBatches.Should().HaveCount(1);
    }
}