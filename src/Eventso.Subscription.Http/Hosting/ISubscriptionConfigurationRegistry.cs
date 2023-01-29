namespace Eventso.Subscription.Http.Hosting;

public interface ISubscriptionConfigurationRegistry
{
    SubscriptionConfiguration Get(string topic);
}