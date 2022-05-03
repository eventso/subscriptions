using System;
using System.Buffers.Binary;
using Confluent.Kafka;

namespace Eventso.Subscription.Kafka.Insights
{
    internal static class KafkaConsumerFactory
    {
        public static KafkaConsumer<T> Create<T>(string bootstrapServers, IDeserializer<T> deserializer)
        {
            if (bootstrapServers == null)
                throw new ArgumentNullException(nameof(bootstrapServers));

            if (deserializer == null)
                throw new ArgumentNullException(nameof(deserializer));

            var config = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = $"insights_{Guid.NewGuid()}",
                AutoOffsetReset = AutoOffsetReset.Error,
                EnableAutoCommit = false,
                EnableAutoOffsetStore = false,
                AllowAutoCreateTopics = false
            };

            var errorWatcher = new ConsumerErrorWatcher();

            var consumer = new ConsumerBuilder<string, T>(config)
                .SetKeyDeserializer(StringKeyDeserializer.Instance)
                .SetValueDeserializer(deserializer)
                .SetErrorHandler((_, error) => errorWatcher.SetError(error))
                .Build();

            return new(consumer, errorWatcher);
        }

        private sealed class StringKeyDeserializer : IDeserializer<string>
        {
            public static readonly StringKeyDeserializer Instance = new();

            public string Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
            {
                return data.Length switch
                {
                    0 => string.Empty,
                    2 => BinaryPrimitives.ReadInt16BigEndian(data).ToString(),
                    4 => BinaryPrimitives.ReadInt32BigEndian(data).ToString(),
                    8 => BinaryPrimitives.ReadInt64BigEndian(data).ToString(),
                    16 => new Guid(data).ToString(),
                    _ => Convert.ToHexString(data)
                };
            }
        }
    }
}