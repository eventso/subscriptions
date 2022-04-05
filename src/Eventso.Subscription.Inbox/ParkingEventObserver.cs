using System;
using System.Threading;
using System.Threading.Tasks;
using Eventso.Subscription.Configurations;
using Eventso.Subscription.Observing;
using Microsoft.Extensions.Logging;

namespace Eventso.Subscription.Inbox
{
    public sealed class ParkingEventObserver<T> : IObserver<T>
        where T : IEvent
    {
        private readonly IObserver<T> _observer;

        public ParkingEventObserver(IObserver<T> observer)
        {
            _observer = observer;
        }

        public async Task OnEventAppeared(T @event, CancellationToken token)
        {
            try
            {
                await _observer.OnEventAppeared(@event, token);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                if (token.IsCancellationRequested)
                    throw;

                await Park(@event.GetKey(), @event.GetMessage(), ex);
            }
        }

        public void Reset()
        {
            throw new System.NotImplementedException();
        }

        private Task Park(Guid key, object value, Exception ex)
        {
            throw new System.NotImplementedException();
        }
    }
}