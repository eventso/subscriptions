using System;
using Eventso.Subscription.Kafka;

namespace Eventso.Subscription.Hosting
{
    public sealed class SubscriptionConfiguration
    {
        public SubscriptionConfiguration(
            ConsumerSettings settings,
            int consumerInstances = 1,
            params TopicSubscriptionConfiguration[] topics)
        {
            if (consumerInstances < 1)
                throw new ArgumentOutOfRangeException(
                    nameof(consumerInstances),
                    consumerInstances,
                    "Instances should be >= 1.");

            Settings = settings ?? throw new ArgumentNullException(nameof(settings));
            ConsumerInstances = consumerInstances;
            Topics = topics;
        }

        public SubscriptionConfiguration(
            ConsumerSettings settings,
            params TopicSubscriptionConfiguration[] topics)
            : this(settings, consumerInstances: 1, topics)
        {
        }
        
        public int ConsumerInstances { get; }

        public TopicSubscriptionConfiguration[] Topics { get; }

        public ConsumerSettings Settings { get; }
    }
}