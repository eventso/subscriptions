namespace Eventso.Subscription;

public interface IObserverFactory<T> where T : IEvent
{
    IObserver<T> Create(IConsumer<T> consumer, string topic);
}
