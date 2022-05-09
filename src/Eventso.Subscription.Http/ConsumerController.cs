using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Eventso.Subscription.Http.Hosting;
using Microsoft.AspNetCore.Mvc;

namespace Eventso.Subscription.Http
{
    [Route("api/consumer")]
    [ApiController]
    public sealed class ConsumerController : ControllerBase
    {
        private readonly ISubscriptionConfigurationRegistry _subscriptionConfigurationRegistry;
        private readonly IMessagePipelineFactory _messagePipelineFactory;
        private readonly IMessageHandlersRegistry _messageHandlersRegistry;

        public ConsumerController(
            ISubscriptionConfigurationRegistry subscriptionConfigurationRegistry,
            IMessagePipelineFactory messagePipelineFactory,
            IMessageHandlersRegistry messageHandlersRegistry)
        {
            _subscriptionConfigurationRegistry = subscriptionConfigurationRegistry;
            _messagePipelineFactory = messagePipelineFactory;
            _messageHandlersRegistry = messageHandlersRegistry;
        }

        [HttpPost]
        [Route("publish/{topic}")]
        public async Task<IActionResult> Publish(string topic)
        {
            var subscriptionConfiguration = _subscriptionConfigurationRegistry.Get(topic);

            var deserializer = new ValueObjectDeserializer(
                subscriptionConfiguration.Deserializer,
                _messageHandlersRegistry);

            var messageBytes = await GetMessageBytes();
            var consumedMessage = deserializer.Deserialize(messageBytes.AsSpan(), topic);

            var observerFactory = new ObserverFactory(
                subscriptionConfiguration,
                _messagePipelineFactory,
                _messageHandlersRegistry);

            var consumer = new Consumer(topic);
            var observer = observerFactory.Create(consumer);

            var message = new Event(consumedMessage);

            await observer.OnEventAppeared(message, CancellationToken.None);

            return Ok();
        }

        private async Task<byte[]> GetMessageBytes()
        {
            await using var memoryStream = new MemoryStream();
            await Request.Body.CopyToAsync(memoryStream);
            return memoryStream.ToArray();
        }

        private sealed class ValueObjectDeserializer
        {
            private readonly IMessageDeserializer _deserializer;
            private readonly IMessageHandlersRegistry _registry;

            public ValueObjectDeserializer(
                IMessageDeserializer deserializer,
                IMessageHandlersRegistry registry)
            {
                _deserializer = deserializer;
                _registry = registry;
            }

            public ConsumedMessage Deserialize(ReadOnlySpan<byte> data, string topic)
            {
                try
                {
                    var context = new DeserializationContext(topic, _registry);
                    return _deserializer.Deserialize(data, context);
                }
                catch (Exception exception)
                {
                    throw new InvalidEventException(
                        topic,
                        $"Can't deserialize message. Deserializer type {_deserializer.GetType().Name}",
                        exception);
                }
            }

            private readonly struct DeserializationContext : IDeserializationContext
            {
                private readonly IMessageHandlersRegistry _registry;

                public DeserializationContext(string topic, IMessageHandlersRegistry registry)
                {
                    Topic = topic;
                    _registry = registry;
                }

                public int HeadersCount => 0;

                public string Topic { get; }

                public (string name, byte[] value) GetHeader(int index) => default;

                public byte[] GetHeaderValue(string name) => null;

                public bool IsHandlerRegisteredFor(Type messageType) =>
                    _registry.ContainsHandlersFor(messageType, out _);
            }
        }
    }
}