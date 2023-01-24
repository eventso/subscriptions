namespace Eventso.Subscription.IntegrationTests;

public interface IKeyedMessage
{
    int Key { get; }
}