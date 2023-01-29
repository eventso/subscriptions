namespace Eventso.Subscription;

public interface IMessageHandlerScope : IDisposable
{
    IEnumerable<IMessageHandler<T>> Resolve<T>();
}