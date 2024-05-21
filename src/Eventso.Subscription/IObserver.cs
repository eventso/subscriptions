namespace Eventso.Subscription;

public interface IObserver<in T> : IDisposable
    where T : IEvent
{
    Task OnEventAppeared(T @event, CancellationToken token);
}