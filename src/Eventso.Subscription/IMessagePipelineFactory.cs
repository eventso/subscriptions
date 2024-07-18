using Eventso.Subscription.Configurations;

namespace Eventso.Subscription;

public interface IMessagePipelineFactory
{
    IMessagePipelineAction Create(HandlerConfiguration config, bool withDlq);
}