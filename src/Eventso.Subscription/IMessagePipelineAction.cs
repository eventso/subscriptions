using System.Threading;
using System.Threading.Tasks;

namespace Eventso.Subscription
{
    public interface IMessagePipelineAction
    {
        Task Invoke<T>(T message, CancellationToken token);
    }
}