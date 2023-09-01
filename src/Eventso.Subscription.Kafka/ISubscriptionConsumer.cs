namespace Eventso.Subscription.Kafka;

public interface ISubscriptionConsumer : IDisposable
{
    Task Consume(CancellationToken token);
    void Close();

    void Pause(string topic);
    void Resume(string topic);
}