using System;
using System.Collections;
using System.Collections.Generic;
using Eventso.Subscription.Configurations;

namespace Eventso.Subscription.WebApi.Hosting
{
    public sealed class SubscriptionCollection : ISubscriptionCollection, ISubscriptionConfigurationRegistry
    {
        private readonly Dictionary<string, SubscriptionConfiguration> _configurations = new();

        public ISubscriptionCollection Add(string topic, IMessageDeserializer deserializer)
        {
            var deferredAckConfiguration = new DeferredAckConfiguration
            {
                Timeout = TimeSpan.Zero
            };

            var configuration = new SubscriptionConfiguration(topic, deserializer, deferredAckConfiguration);
            Add(configuration);

            return this;
        }
        
        public ISubscriptionCollection AddBatch(
            string topic,
            IMessageDeserializer deserializer,
            TimeSpan? batchTriggerTimeout = null)
        {
            var batchConfiguration = new BatchConfiguration
            {
                BatchTriggerTimeout = batchTriggerTimeout ?? TimeSpan.Zero,
                HandlingStrategy = BatchHandlingStrategy.OrderedWithinKey
            };

            var subscription = new SubscriptionConfiguration(topic, deserializer, batchConfiguration);
            Add(subscription);

            return this;
        }

        public SubscriptionConfiguration Get(string topic)
        {
            if (!_configurations.TryGetValue(topic, out var configuration))
                throw new ArgumentException($"Subscription to '{topic}' topic is not found.");

            return configuration;
        }

        public IEnumerator<SubscriptionConfiguration> GetEnumerator() => _configurations.Values.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        private ISubscriptionCollection Add(SubscriptionConfiguration configuration)
        {
            if (_configurations.ContainsKey(configuration.Topic))
                throw new ArgumentException(
                    $"Multiple subscriptions with the same topic '{configuration.Topic}' "
                    + "are not supported and may lead to unexpected behaviour.");
            
            _configurations.Add(configuration.Topic, configuration);
        
            return this;
        }
    }
}