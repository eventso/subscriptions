namespace Eventso.Subscription.Observing;

internal static class EventExtensions
{
    public static bool CanSkip<T>(
        this T message,
        bool skipUnknown) where T : IEvent
    {
        return message.DeserializationResult
            switch
            {
                DeserializationStatus.UnknownType when skipUnknown => true,
                DeserializationStatus.Skipped => true,
                DeserializationStatus.UnknownType => throw new InvalidEventException(
                    message.GetIdentity(),
                    "Unknown message received with skipUnknown=false"),
                _ => false
            };
    }
}