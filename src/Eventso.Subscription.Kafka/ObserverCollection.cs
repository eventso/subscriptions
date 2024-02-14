namespace Eventso.Subscription.Kafka;

internal sealed class ObserverCollection : IDisposable
{
    private readonly IObserverFactory _factory;
    private readonly IConsumer<Event> _consumer;
    private readonly Dictionary<TopicKey, IObserver<Event>> _items = new();

    public ObserverCollection(
        IObserverFactory factory,
        IConsumer<Event> consumer)
    {
        _factory = factory;
        _consumer = consumer;
    }

    public IObserver<Event> Get(TopicKey key)
    {
        if (_items.TryGetValue(key, out var observer))
            return observer;

        observer = _factory.Create(_consumer, key.Topic);
        _items.Add(key, observer);

        return observer;
    }

    public void Dispose()
    {
        foreach (IObserver<Event> observer in _items.Values)
            if (observer is IDisposable disposable)
                disposable.Dispose();
    }
}