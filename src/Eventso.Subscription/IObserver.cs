using System.Threading;
using System.Threading.Tasks;

namespace Eventso.Subscription
{
    public interface IObserver<in T> where T : IEvent
    {
        Task OnEventAppeared(T @event, CancellationToken token);
    }
}