using System;
using Eventso.Subscription.Configurations;

namespace Eventso.Subscription.InMemory.Hosting
{
    public sealed class SubscriptionConfiguration
    {
        private SubscriptionConfiguration(string topic, IMessageDeserializer deserializer)
        {
            Topic = string.IsNullOrWhiteSpace(topic)
                ? throw new ArgumentException("Topic name is not specified.")
                : topic;

            Deserializer = deserializer ?? throw new ArgumentNullException(nameof(deserializer));
        }
        
        public SubscriptionConfiguration(
            string topic,
            IMessageDeserializer deserializer,
            DeferredAckConfiguration deferredAckConfiguration)
            : this(topic, deserializer)
        {
            deferredAckConfiguration?.Validate();

            DeferredAckConfiguration = deferredAckConfiguration ?? new DeferredAckConfiguration();
        }

        public SubscriptionConfiguration(
            string topic,
            IMessageDeserializer deserializer,
            BatchConfiguration batchConfiguration)
            : this(topic, deserializer)
        {
            batchConfiguration.Validate();

            BatchConfiguration = batchConfiguration;
            BatchProcessingRequired = true;
        }

        public string Topic { get; }

        public IMessageDeserializer Deserializer { get; }

        public DeferredAckConfiguration DeferredAckConfiguration { get; }

        public BatchConfiguration BatchConfiguration { get; }

        public bool BatchProcessingRequired { get; }

        public HandlerConfiguration HandlerConfiguration { get; } = new();
    }
}