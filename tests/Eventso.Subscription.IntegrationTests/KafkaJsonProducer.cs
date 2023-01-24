using Confluent.Kafka;
using SpanJson;

namespace Eventso.Subscription.IntegrationTests;

public sealed class KafkaJsonProducer : IDisposable
{
    private readonly IProducer<int, byte[]> _producer;

    public KafkaJsonProducer(KafkaConfig config)
    {
        var builder = new ProducerBuilder<int, byte[]>(
            new ProducerConfig
            {
                BootstrapServers = config.Brokers,
                Acks = Acks.All,
                EnableIdempotence = true,
                MaxInFlight = 1,
                MessageSendMaxRetries = 5,
                LingerMs = 10,
                CompressionType = CompressionType.Lz4
            });

        _producer = builder.Build();
    }

    public Task Publish<T>(string topic, T message, CancellationToken token = default)
        where T : IKeyedMessage
    {
        return Publish(topic, message.Key, JsonSerializer.Generic.Utf8.Serialize(message), token);
    }

    public Task Publish<T>(string topic, IEnumerable<T> messages, CancellationToken token = default)
        where T : IKeyedMessage
    {
        var tasks = messages.Select(m => Publish(
            topic,
            m.Key,
            JsonSerializer.Generic.Utf8.Serialize(m),
            token));

        return Task.WhenAll(tasks);
    }

    public Task Publish(string topic, int key, byte[] data, CancellationToken token = default)
        => _producer.ProduceAsync(topic, new Message<int, byte[]> { Key = key, Value = data }, token);

    public Task Publish(string topic, IEnumerable<(int key, byte[] data)> items, CancellationToken token = default)
    {
        var tasks = items.Select(i => _producer.ProduceAsync(
            topic,
            new Message<int, byte[]> { Key = i.key, Value = i.data },
            token));

        return Task.WhenAll(tasks);
    }

    public void Dispose()
    {
        _producer.Dispose();
    }
}