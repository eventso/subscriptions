namespace Eventso.Subscription.Observing;

public sealed class DelayedEventObserver<TEvent> : IObserver<TEvent>, IDisposable
    where TEvent : IEvent
{
    private readonly TimeSpan _delay;
    private readonly IObserver<TEvent> _inner;

    public DelayedEventObserver(TimeSpan delay, IObserver<TEvent> inner)
    {
        _delay = delay.Duration();
        _inner = inner;
    }

    public async Task OnEventAppeared(TEvent @event, CancellationToken token)
    {
        var eventDelay = DateTime.UtcNow - @event.GetUtcTimestamp();

        if (eventDelay < _delay)
            await Task.Delay(_delay - eventDelay, token);

        await _inner.OnEventAppeared(@event, token);
    }

    public void Dispose()
    {
        (_inner as IDisposable)?.Dispose();
    }
}
