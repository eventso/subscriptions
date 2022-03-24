namespace Eventso.Subscription
{
    public interface IMessageHandlerScopeFactory
    {
        IMessageHandlerScope BeginScope();
    }
}