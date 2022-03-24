using System;

namespace Eventso.Subscription
{
    public class InvalidMessageException : MessageHandlingException
    {
        public InvalidMessageException(
            string messageSource,
            string message,
            Exception innerException = null)
            : base(messageSource, message, innerException)
        {
        }
    }
}