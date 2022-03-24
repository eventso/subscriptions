using System.Threading;
using System.Threading.Tasks;

namespace Eventso.Subscription
{
    public interface IObserver<in T> where T : IMessage
    {
        Task OnMessageAppeared(T message, CancellationToken token);

        Task OnMessageTimeout(CancellationToken token)
            => Task.CompletedTask;
    }
}