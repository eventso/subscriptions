using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Confluent.Kafka;
using Eventso.Subscription.Hosting;
using Microsoft.AspNetCore.Mvc;

namespace Eventso.Subscription.Kafka.Insights
{
    [Route("insights/kafka")]
    [ApiController]
    public sealed class KafkaController : ControllerBase
    {
        private readonly IEnumerable<SubscriptionConfiguration> _subscriptionConfigurations;

        public KafkaController(IEnumerable<ISubscriptionCollection> subscriptions)
        {
            if (subscriptions == null)
                throw new ArgumentNullException(nameof(subscriptions));

            _subscriptionConfigurations = subscriptions.SelectMany(i => i);
        }

        [HttpGet("{topic}/{partition:int}/{offset:long}")]
        public Message Get(string topic, int partition, long offset, CancellationToken token)
        {
            if (!TryGetSubscriptionConfiguration(topic, out var configuration))
                throw new ArgumentException($"Subscription to '{topic} not found.'");

            var valueObjectDeserializer = new ValueObjectDeserializer(
                configuration.Serializer,
                MessageHandlerRegistry.Instance);

            using var consumer = KafkaConsumerFactory.Create(
                configuration.Settings.Config.BootstrapServers,
                valueObjectDeserializer);

            var requestedOffset = new TopicPartitionOffset(topic, partition, offset);

            var result = consumer.Consume(requestedOffset, token);

            if (result == null)
                return null;

            return new(result);
        }

        [HttpGet("{topic}/{partition:int}/{offset:long}/raw")]
        public Message GetRaw(string topic, int partition, long offset, CancellationToken token)
        {
            if (!TryGetSubscriptionConfiguration(topic, out var configuration))
                throw new ArgumentException($"Subscription to '{topic} not found.'");

            using var consumer = KafkaConsumerFactory.Create(
                configuration.Settings.Config.BootstrapServers,
                Deserializers.Utf8);

            var requestedOffset = new TopicPartitionOffset(topic, partition, offset);

            var result = consumer.Consume(requestedOffset, token);

            if (result == null)
                return null;

            return new(result);
        }

        private bool TryGetSubscriptionConfiguration(string topic, out SubscriptionConfiguration configuration)
        {
            configuration = _subscriptionConfigurations
                .SingleOrDefault(i => i.Settings.Topic.Equals(topic, StringComparison.OrdinalIgnoreCase));

            return configuration != null;
        }

        public sealed record Message(
            object value,
            string key,
            string topic,
            int partition,
            long offset,
            Headers headers,
            DateTime timestamp)
        {
            public Message(ConsumeResult<string, ConsumedMessage> result)
                : this(result.Message.Value.Message,
                    result.Message.Key,
                    result.Topic,
                    result.Partition,
                    result.Offset,
                    result.Message.Headers,
                    result.Message.Timestamp.UtcDateTime)
            {
            }

            public Message(ConsumeResult<string, string> result)
                : this(result.Message.Value,
                    result.Message.Key,
                    result.Topic,
                    result.Partition,
                    result.Offset,
                    result.Message.Headers,
                    result.Message.Timestamp.UtcDateTime)
            {
            }
        };

        private sealed class MessageHandlerRegistry : IMessageHandlersRegistry
        {
            public static readonly IMessageHandlersRegistry Instance = new MessageHandlerRegistry();

            public bool ContainsHandlersFor(Type messageType, out HandlerKind kind)
            {
                kind = default;
                return true;
            }
        }
    }
}