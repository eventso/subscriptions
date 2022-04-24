using System.Runtime.InteropServices;

namespace Eventso.Subscription.Observing.DeadLetter
{
    [StructLayout(LayoutKind.Auto)]
    public readonly struct PoisonEvent<TEvent>
        where TEvent : IEvent
    {
        public PoisonEvent(TEvent @event, string reason)
        {
            Event = @event;
            Reason = reason;
        }

        public TEvent Event { get; }

        public string Reason { get; }
    }
}