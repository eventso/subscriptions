namespace Eventso.Subscription;

public class EventHandlingException : Exception
{
    public EventHandlingException(string eventSource, string message, Exception? innerException)
        : base($"{message} Source: '{eventSource}'", innerException)
    {
    }
}