using System.Threading;
using System.Threading.Tasks;

namespace Eventso.Subscription.Observing.Batch
{
    public interface IBatchHandler<in T>
        where T : IMessage
    {
        Task Handle(IConvertibleCollection<T> messages, CancellationToken token);
    }
}