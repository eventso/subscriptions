using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Eventso.Subscription.Kafka;
using System.Collections.Concurrent;

namespace Eventso.Subscription.IntegrationTests;

public sealed class TopicSource : IAsyncDisposable
{
    private readonly KafkaConfig _config;
    private readonly KafkaJsonProducer _producer;
    private readonly IAdminClient _adminClient;

    private readonly List<string> _topics = new();

    public const int NumPartitions = 3;

    public TopicSource(KafkaConfig config, KafkaJsonProducer producer)
    {
        _config = config;
        _producer = producer;
        _adminClient = new AdminClientBuilder(
                new AdminClientConfig
                {
                    BootstrapServers = config.Brokers,
                })
            .Build();
    }

    public async Task<string> CreateTopic()
    {
        var name = $"test_{Guid.NewGuid():N}";

        await _adminClient.CreateTopicsAsync(
            new[]
            {
                new TopicSpecification
                {
                    Name = name,
                    NumPartitions = NumPartitions
                }
            },
            new CreateTopicsOptions()
            {
                OperationTimeout = TimeSpan.FromMilliseconds(500)
            });


        _topics.Add(name);

        return name;
    }

    public async Task<(string topic, T[] messages)> CreateTopicWithMessages<T>(IFixture fixture, int count)
        where T : IKeyedMessage
    {
        var topic = await CreateTopic();
        var messages = fixture.CreateMany<T>(count).ToArray();
        await _producer.Publish(topic, messages);

        return (topic, messages);
    }

    public IEnumerable<TopicPartitionOffset> GetCommittedOffsets(string topic, string groupId)
    {
        using var consumer = new ConsumerBuilder<Ignore, Ignore>(((KafkaConsumerSettings)_config).Config)
            .Build();

        return consumer.Committed(
            Enumerable
                .Range(0, NumPartitions)
                .Select(p => new TopicPartition(topic, p)),
            TimeSpan.FromMinutes(1));
    }

    public IEnumerable<(int partition, long lag)> GetLag(string topic, string groupId)
    {
        KafkaConsumerSettings config = _config with { GroupId = groupId };
        using var consumer = new ConsumerBuilder<Ignore, Ignore>(config.Config)
            .Build();

        var offsets = consumer.Committed(
            Enumerable
                .Range(0, NumPartitions)
                .Select(p => new TopicPartition(topic, p)),
            TimeSpan.FromMinutes(1));

        return offsets
            .Select(o => (
                o.Partition.Value,
                consumer.QueryWatermarkOffsets(o.TopicPartition, TimeSpan.FromSeconds(1)).High - o.Offset))
            .ToArray();
    }

    public async ValueTask DisposeAsync()
    {
        if (_topics.Count == 0)
            return;

        await _adminClient.DeleteTopicsAsync(
            _topics,
            new DeleteTopicsOptions
            {
                OperationTimeout = TimeSpan.FromMilliseconds(500)
            });

        _adminClient.Dispose();
        _topics.Clear();
    }
}