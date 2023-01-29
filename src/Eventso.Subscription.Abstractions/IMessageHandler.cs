namespace Eventso.Subscription;

public interface IMessageHandler<in TMessage>
{
    Task Handle(TMessage message, CancellationToken token);
}