namespace Eventso.Subscription.Observing
{
    internal static class MessageExtensions
    {
        public static bool CanSkip<T>(
            this T message,
            bool skipUnknown) where T : IMessage
        {
            return message.DeserializationResult
                switch
                {
                    DeserializationStatus.UnknownType when skipUnknown => true,
                    DeserializationStatus.Skipped => true,
                    DeserializationStatus.UnknownType => throw new InvalidMessageException(
                        message.GetIdentity(),
                        "Unknown message received with skipUnknown=false"),
                    _ => false
                };
        }
    }
}