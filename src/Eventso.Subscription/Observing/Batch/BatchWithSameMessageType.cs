using System;

namespace Eventso.Subscription.Observing.Batch
{
    public readonly struct BatchWithSameMessageType<TEvent> : IDisposable
        where TEvent : IEvent
    {
        private readonly Type _type;
        private readonly PooledList<TEvent> _events;

        public BatchWithSameMessageType(TEvent @event)
        {
            _type = @event.GetMessage().GetType();
            _events = new PooledList<TEvent>(4);
            _events.Add(@event);
        }

        public bool TryAdd(TEvent @event)
        {
            if (!HasEqualType(@event))
                return false;

            _events.Add(@event);
            return true;
        }

        public IConvertibleCollection<TEvent> Events
            => _events;

        private bool HasEqualType(TEvent @event)
            => _type == @event.GetMessage().GetType();

        public void Dispose()
            => _events?.Dispose();
    }
}