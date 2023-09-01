namespace Eventso.Subscription.Hosting;

public interface ISubscriptionHost
{
    void Pause(string topic, string? groupId = null);
    void Resume(string topic, string? groupId = null);
}