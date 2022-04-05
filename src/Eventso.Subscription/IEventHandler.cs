using System.Threading;
using System.Threading.Tasks;

namespace Eventso.Subscription
{
    public interface IEventHandler<in TEvent>
        where TEvent : IEvent
    {
        Task Handle(string topic, TEvent @event, CancellationToken cancellationToken);
        
        /// <remarks>
        /// Expects <paramref name="events"/> of single message type. 
        /// </remarks>
        Task Handle(string topic, IConvertibleCollection<TEvent> events, CancellationToken cancellationToken);
    }
}