using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Eventso.Subscription.Observing.Batch
{
    public sealed class SingleTypeLastByKeyEventHandler<TEvent> : IEventHandler<TEvent>
        where TEvent : IEvent
    {
        private readonly IEventHandler<TEvent> _nextHandler;

        public SingleTypeLastByKeyEventHandler(IEventHandler<TEvent> nextHandler)
            => _nextHandler = nextHandler;

        public Task Handle(TEvent @event, CancellationToken cancellationToken)
            => _nextHandler.Handle(@event, cancellationToken);

        public Task Handle(IConvertibleCollection<TEvent> events, CancellationToken token)
        {
            if (events.Count == 0)
                return Task.CompletedTask;

            var dictionary = new Dictionary<Guid, TEvent>(events.Count);

            for (var i = 0; i < events.Count; ++i)
            {
                var @event = events[i];
                dictionary[@event.GetKey()] = @event;
            }

            using var lastEvents = new PooledList<TEvent>(dictionary.Count);
            foreach (var (_, @event) in dictionary)
                lastEvents.Add(@event);

            return _nextHandler.Handle(lastEvents, token);
        }
    }
}