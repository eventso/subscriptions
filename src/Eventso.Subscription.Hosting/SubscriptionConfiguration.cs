using System;
using Eventso.Subscription.Configurations;
using Eventso.Subscription.Kafka;

namespace Eventso.Subscription.Hosting
{
    public sealed class SubscriptionConfiguration
    {
        private SubscriptionConfiguration(
            ConsumerSettings settings,
            IMessageDeserializer serializer,
            HandlerConfiguration handlerConfig = default,
            bool skipUnknownMessages = true,
            int consumerInstances = 1)
        {
            if (settings == null)
                throw new ArgumentNullException(nameof(settings));

            if (consumerInstances < 1)
                throw new ArgumentOutOfRangeException(
                    nameof(consumerInstances),
                    consumerInstances,
                    "Instances should be >= 1.");

            if (string.IsNullOrWhiteSpace(settings.Topic))
                throw new ArgumentException("Topic name is not specified.");

            Settings = settings;
            Serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            SkipUnknownMessages = skipUnknownMessages;
            ConsumerInstances = consumerInstances;
            HandlerConfig = handlerConfig ?? new HandlerConfiguration();
        }

        public SubscriptionConfiguration(
            ConsumerSettings settings,
            IMessageDeserializer serializer,
            HandlerConfiguration handlerConfig = default,
            bool skipUnknownMessages = true,
            int consumerInstances = 1,
            DeferredAckConfiguration deferredAckConfiguration = default)
            : this(settings,
                serializer,
                handlerConfig,
                skipUnknownMessages,
                consumerInstances)
        {
            deferredAckConfiguration?.Validate();

            DeferredAckConfiguration = deferredAckConfiguration ?? new DeferredAckConfiguration();
        }

        public SubscriptionConfiguration(
            ConsumerSettings settings,
            BatchConfiguration batchConfiguration,
            IMessageDeserializer serializer,
            HandlerConfiguration handlerConfig = default,
            bool skipUnknownMessages = true,
            int consumerInstances = 1)
            : this(settings,
                serializer,
                handlerConfig,
                skipUnknownMessages,
                consumerInstances)
        {
            batchConfiguration.Validate();
            
            BatchConfiguration = batchConfiguration;
            BatchProcessingRequired = true;
        }

        public int ConsumerInstances { get; }

        public bool BatchProcessingRequired { get; }

        public bool SkipUnknownMessages { get; }

        public ConsumerSettings Settings { get; }

        public IMessageDeserializer Serializer { get; }

        public HandlerConfiguration HandlerConfig { get; }

        public BatchConfiguration BatchConfiguration { get; }
        
        public DeferredAckConfiguration DeferredAckConfiguration { get; }
    }
}