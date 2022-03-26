using System.Threading;
using System.Threading.Tasks;

namespace Eventso.Subscription.Observing.Batch
{
    public interface IBatchHandler<in TEvent>
        where TEvent : IEvent
    {
        Task Handle(IConvertibleCollection<TEvent> events, CancellationToken token);
    }
}