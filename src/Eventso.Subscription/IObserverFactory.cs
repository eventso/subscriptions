namespace Eventso.Subscription
{
    public interface IObserverFactory
    {
        IObserver<T> Create<T>(IConsumer<T> consumer, string topic) where T : IEvent;
    }
}