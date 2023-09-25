namespace Eventso.Subscription.Tests;

public sealed class BufferedObserverTests
{
    private readonly Fixture _fixture;
    private readonly List<TestEvent> _handledEvents = new();
    private readonly IObserver<TestEvent> _testObserver;

    public BufferedObserverTests()
    {
        _fixture = new Fixture();
        _fixture.Customize(new AutoNSubstituteCustomization { ConfigureMembers = true });
        _testObserver = Substitute.For<IObserver<TestEvent>>();
        _testObserver.OnEventAppeared(Arg.Do<TestEvent>(_handledEvents.Add), Arg.Any<CancellationToken>())
            .Returns(Task.Delay(50));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(10)]
    [InlineData(20)]
    public async Task ObservingEvents_AllPassedToNext(int eventCount)
    {
        var observer = new BufferedObserver<TestEvent>(
            10,
            _testObserver,
            CancellationToken.None);

        var events = _fixture.CreateMany<TestEvent>(eventCount).ToArray();

        foreach (var @event in events)
            await observer.OnEventAppeared(@event, CancellationToken.None);

        await observer.Complete();

        _handledEvents.Should().HaveSameCount(events);
    }

    [Fact]
    public async Task ObservingEvents_CapacityLimitBuffer()
    {
        const int capacity = 10;

        var tcs = new TaskCompletionSource();

        _testObserver.OnEventAppeared(default!, default)
            .ReturnsForAnyArgs(tcs.Task);

        var observer = new BufferedObserver<TestEvent>(
            capacity,
            _testObserver,
            CancellationToken.None);

        var events = _fixture.CreateMany<TestEvent>(capacity * 2).ToArray();

        var count = 0;

        foreach (var @event in events)
        {
            var task = observer.OnEventAppeared(@event, CancellationToken.None);
            await Task.Delay(30);
            if (!task.IsCompleted)
                break;

            count++;
        }

        count.Should().Be(capacity + 1);
    }

    [Fact]
    public async Task ObservingEvents_CanBeCancelled()
    {
        const int capacity = 10;

        var tcs = new TaskCompletionSource();
        using var cts = new CancellationTokenSource();

        _testObserver.OnEventAppeared(default!, default)
            .ReturnsForAnyArgs(tcs.Task);

        var observer = new BufferedObserver<TestEvent>(
            capacity,
            _testObserver,
            cts.Token);

        var events = _fixture.CreateMany<TestEvent>(capacity).ToArray();

        foreach (var @event in events)
            await observer.OnEventAppeared(@event, CancellationToken.None);

        cts.Cancel();
        tcs.SetResult();
        var act = () => observer.Complete();

        await act.Should().ThrowAsync<OperationCanceledException>();

        var act2 = () => observer.OnEventAppeared(events[0], CancellationToken.None);
        await act2.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task ObservingEvents_FaultedOnInnerException()
    {
        const int capacity = 10;

        var tcs = new TaskCompletionSource();

        _testObserver.OnEventAppeared(default!, default)
            .ThrowsAsyncForAnyArgs(new CustomException());

        var observer = new BufferedObserver<TestEvent>(
            capacity,
            _testObserver,
            CancellationToken.None);

        var events = _fixture.CreateMany<TestEvent>(capacity).ToArray();

        await observer.OnEventAppeared(events[0], CancellationToken.None);
        await Task.Delay(100);

        var act = () => observer.OnEventAppeared(events[1], CancellationToken.None);

        await act.Should().ThrowAsync<CustomException>();
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

    private class CustomException : Exception
    {
    }
}