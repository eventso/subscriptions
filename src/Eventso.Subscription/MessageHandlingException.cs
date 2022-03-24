using System;

namespace Eventso.Subscription
{
    public class MessageHandlingException : Exception
    {
        public MessageHandlingException(string messageSource, string message, Exception innerException)
            : base($"{message} Source: '{messageSource}'", innerException)
        {
        }
    }
}