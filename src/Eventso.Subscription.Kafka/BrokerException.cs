using System;

namespace Eventso.Subscription.Kafka
{
    public sealed class BrokerException : Exception
    {
        public BrokerException(Exception innerException)
            : base(innerException.Message, innerException)
        {
        }
    }
}