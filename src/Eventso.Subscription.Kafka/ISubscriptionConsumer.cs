public interface ISubscriptionConsumer : IDisposable
{
    Task Consume(CancellationToken token);
    void Close();
}