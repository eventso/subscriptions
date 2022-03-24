using System;

namespace Eventso.Subscription
{
    public interface IDeserializationContext
    {
        int HeadersCount { get; }
        (string name, byte[] value) GetHeader(int index);
        byte[] GetHeaderValue(string name);

        public string Topic { get; }

        /// <summary>
        /// Returns true if there is any registered handler for messageType.
        /// </summary>
        bool IsHandlerRegisteredFor(Type messageType);
    }
}