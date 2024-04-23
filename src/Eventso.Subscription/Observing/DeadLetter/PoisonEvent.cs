using System.Runtime.InteropServices;

namespace Eventso.Subscription.Observing.DeadLetter;

[StructLayout(LayoutKind.Auto)]
public readonly record struct PoisonEvent<TEvent>
    where TEvent : IEvent
{
    internal PoisonEvent(TEvent @event, string reason)
    {
        Event = @event;
        Reason = reason;
    }

    public TEvent Event { get; }

    public string Reason { get; }
}