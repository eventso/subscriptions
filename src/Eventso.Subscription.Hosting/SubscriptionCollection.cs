using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Eventso.Subscription.Configurations;
using Eventso.Subscription.Kafka;

namespace Eventso.Subscription.Hosting
{
    public sealed class SubscriptionCollection : ISubscriptionCollection
    {
        private readonly List<SubscriptionConfiguration> _subscriptions = new List<SubscriptionConfiguration>();

        public ISubscriptionCollection Add(
            ConsumerSettings settings,
            IMessageDeserializer serializer,
            HandlerConfiguration handlerConfig = default,
            DeferredAckConfiguration deferredAckConfig = default,
            bool skipUnknownMessages = true,
            int instances = 1,
            bool enableDeadLetterQueue = false)
        {
            var subscription = new SubscriptionConfiguration(
                settings,
                serializer,
                handlerConfig,
                skipUnknownMessages,
                instances,
                deferredAckConfig,
                enableDeadLetterQueue);

            Add(subscription);

            return this;
        }

        public ISubscriptionCollection AddBatch(
            ConsumerSettings settings,
            BatchConfiguration batchConfig,
            IMessageDeserializer serializer,
            HandlerConfiguration handlerConfig = default,
            bool skipUnknownMessages = true,
            int instances = 1,
            bool enableDeadLetterQueue = false)
        {
            var subscription = new SubscriptionConfiguration(
                settings,
                batchConfig,
                serializer,
                handlerConfig,
                skipUnknownMessages,
                instances,
                enableDeadLetterQueue);

            Add(subscription);

            return this;
        }

        public ISubscriptionCollection Add(SubscriptionConfiguration configuration)
        {
            var hasDuplicates = _subscriptions.Any(s =>
                string.Equals(s.Settings.Topic, configuration.Settings.Topic, StringComparison.OrdinalIgnoreCase) &&
                !string.IsNullOrEmpty(s.Settings.Config.GroupId) &&
                string.Equals(s.Settings.Config.GroupId, configuration.Settings.Config.GroupId));

            if (hasDuplicates)
                throw new ArgumentException(
                    $"Multiple subscriptions with the same topic '{configuration.Settings.Topic}' " +
                    $"and consumer group '{configuration.Settings.Config.GroupId}'" +
                    "are not supported and may lead to unexpected behaviour. " +
                    "Use instance count instead.");

            _subscriptions.Add(configuration);

            return this;
        }

        public IEnumerator<SubscriptionConfiguration> GetEnumerator()
            => _subscriptions.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}