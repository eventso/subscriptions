using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Eventso.Subscription.Observing.Batch
{
    public interface IMessageBatchPipelineAction
    {
        Task Invoke<T>(IReadOnlyCollection<T> messages, CancellationToken token);
    }
}