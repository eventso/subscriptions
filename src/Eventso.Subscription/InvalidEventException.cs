namespace Eventso.Subscription;

public class InvalidEventException : EventHandlingException
{
    public InvalidEventException(
        string eventSource,
        string message,
        Exception innerException = null)
        : base(eventSource, message, innerException)
    {
    }
}