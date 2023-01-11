using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace Eventso.Subscription.IntegrationTests;

public sealed class TopicSource : IAsyncDisposable
{
    private readonly KafkaJsonProducer _producer;
    private readonly IAdminClient _adminClient;

    private readonly List<string> _topics = new();

    public TopicSource(KafkaConfig config, KafkaJsonProducer producer)
    {
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
                    NumPartitions = 3
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