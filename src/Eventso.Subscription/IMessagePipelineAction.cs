namespace Eventso.Subscription;

public interface IMessagePipelineAction
{
    Task Invoke<T>(T message, HandlingContext context, CancellationToken token) where T : notnull;
}