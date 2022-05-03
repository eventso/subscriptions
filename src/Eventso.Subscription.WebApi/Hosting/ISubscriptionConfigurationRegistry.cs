namespace Eventso.Subscription.WebApi.Hosting
{
    public interface ISubscriptionConfigurationRegistry
    {
        SubscriptionConfiguration Get(string topic);
    }
}