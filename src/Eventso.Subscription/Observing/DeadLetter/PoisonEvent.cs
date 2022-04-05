using System.Runtime.InteropServices;

namespace Eventso.Subscription.Observing.DeadLetter
{
    [StructLayout(LayoutKind.Auto)]
    public readonly struct PoisonEvent<TEvent>
        where TEvent : IEvent
    {
        public PoisonEvent(string topic, TEvent @event, string reason)
        {
            Topic = topic;
            Event = @event;
            Reason = reason;
        }

        public string Topic { get; }

        public TEvent Event { get; }

        public string Reason { get; }
    }
}