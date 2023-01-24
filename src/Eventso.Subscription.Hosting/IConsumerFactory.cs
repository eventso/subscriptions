namespace Eventso.Subscription.Hosting;

public interface IConsumerFactory
{
    ISubscriptionConsumer CreateConsumer(SubscriptionConfiguration config);
}