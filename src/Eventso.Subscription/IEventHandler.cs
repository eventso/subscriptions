using System.Threading;
using System.Threading.Tasks;

namespace Eventso.Subscription
{
    public interface IEventHandler<in TEvent>
        where TEvent : IEvent
    {
        Task Handle(TEvent @event, CancellationToken cancellationToken);
        
        Task Handle(IConvertibleCollection<TEvent> events, CancellationToken cancellationToken);
    }
}