using System.Collections;
using Eventso.Subscription.Kafka;
using Scrutor;

namespace Eventso.Subscription.Hosting;

public sealed class SubscriptionCollection : ISubscriptionCollection
{
    private readonly List<SubscriptionConfiguration> _subscriptions = new();

    public ISubscriptionCollection Add(
        ConsumerSettings settings,
        IMessageDeserializer serializer,
        HandlerConfiguration? handlerConfig = default,
        DeferredAckConfiguration? deferredAckConfig = default,
        bool skipUnknownMessages = true,
        int instances = 1,
        TimeSpan? messageObservingDelay = default)
    {
        var subscription = new SubscriptionConfiguration(
            settings,
            instances,
            new TopicSubscriptionConfiguration(
                settings.Topic,
                serializer,
                handlerConfig,
                deferredAckConfig,
                skipUnknownMessages)
            {
                ObservingDelay = messageObservingDelay
            });

        return Add(subscription);
    }

    public ISubscriptionCollection AddBatch(
        ConsumerSettings settings,
        BatchConfiguration batchConfig,
        IMessageDeserializer serializer,
        HandlerConfiguration? handlerConfig = default,
        bool skipUnknownMessages = true,
        int instances = 1,
        TimeSpan? messageObservingDelay = default)
    {
        var subscription = new SubscriptionConfiguration(
            settings,
            instances,
            new TopicSubscriptionConfiguration(
                settings.Topic,
                batchConfig,
                serializer,
                handlerConfig,
                skipUnknownMessages)
            {
                ObservingDelay = messageObservingDelay
            });

        return Add(subscription);
    }

    public ISubscriptionCollection AddMultiTopic(
        KafkaConsumerSettings settings,
        Action<IMultiTopicSubscriptionCollection> addSubscriptions,
        int instances = 1)
    {
        var collection = new MultiTopicSubscriptionCollection();
        addSubscriptions(collection);

        if (collection.TopicConfigurations.Count == 0)
            throw new ArgumentException("Topics subscriptions required.", nameof(addSubscriptions));

        var subscription = new SubscriptionConfiguration(
            settings,
            instances,
            collection.TopicConfigurations.ToArray());

        return Add(subscription);
    }

    public ISubscriptionCollection Add(SubscriptionConfiguration configuration)
    {
        foreach (var topic in configuration.GetTopics())
        {
            var hasDuplicates = _subscriptions.Any(s =>
                s.Contains(topic) &&
                !string.IsNullOrEmpty(s.Settings.Config.GroupId) &&
                string.Equals(s.Settings.Config.GroupId, configuration.Settings.Config.GroupId));

            if (hasDuplicates)
                throw new ArgumentException(
                    $"Multiple subscriptions with the same topic '{topic}' " +
                    $"and consumer group '{configuration.Settings.Config.GroupId}'" +
                    "are not supported and may lead to unexpected behaviour. " +
                    "Use instance count instead.");
        }

        _subscriptions.Add(configuration);

        return this;
    }

    public IEnumerator<SubscriptionConfiguration> GetEnumerator()
        => _subscriptions.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    private sealed class MultiTopicSubscriptionCollection : IMultiTopicSubscriptionCollection
    {
        internal readonly List<TopicSubscriptionConfiguration> TopicConfigurations = new();

        public IMultiTopicSubscriptionCollection Add(
            string topic,
            IMessageDeserializer serializer,
            HandlerConfiguration? handlerConfig = default,
            DeferredAckConfiguration? deferredAckConfig = default,
            bool skipUnknownMessages = true,
            int bufferSize = 10,
            TimeSpan? messageObservingDelay = default)
        {
            return Add(new TopicSubscriptionConfiguration(
                topic,
                serializer,
                handlerConfig,
                deferredAckConfig,
                skipUnknownMessages,
                bufferSize: bufferSize)
            {
                ObservingDelay = messageObservingDelay
            });
        }

        public IMultiTopicSubscriptionCollection AddBatch(
            string topic,
            BatchConfiguration batchConfig,
            IMessageDeserializer serializer,
            HandlerConfiguration? handlerConfig = default,
            bool skipUnknownMessages = true,
            int bufferSize = 0,
            TimeSpan? messageObservingDelay = default)
        {
            return Add(new TopicSubscriptionConfiguration(
                topic,
                batchConfig,
                serializer,
                handlerConfig,
                skipUnknownMessages,
                bufferSize: bufferSize)
            {
                ObservingDelay = messageObservingDelay
            });
        }

        public IMultiTopicSubscriptionCollection Add(TopicSubscriptionConfiguration configuration)
        {
            var hasDuplicates = TopicConfigurations.Any(c => c.Topic.Equals(configuration.Topic));

            if (hasDuplicates)
                throw new ArgumentException(
                    $"Duplicate topic '{configuration.Topic}' in multi-topic subscription.");

            TopicConfigurations.Add(configuration);

            return this;
        }
    }
}