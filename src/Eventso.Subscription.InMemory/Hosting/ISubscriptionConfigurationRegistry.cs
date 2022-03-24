namespace Eventso.Subscription.InMemory.Hosting
{
    public interface ISubscriptionConfigurationRegistry
    {
        SubscriptionConfiguration Get(string topic);
    }
}