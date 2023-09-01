using Eventso.Subscription.Hosting;
using Eventso.Subscription.SpanJson;

namespace Eventso.Subscription.IntegrationTests.Batch;

public sealed class TriggerHandle : IAsyncLifetime
{
    private readonly KafkaConfig _config;
    private readonly TopicSource _topicSource;
    private readonly TestHostStartup _hostStartup;
    private readonly IFixture _fixture;

    public TriggerHandle(
        KafkaConfig config,
        TopicSource topicSource,
        TestHostStartup hostRunner,
        IFixture fixture)
    {
        _config = config;
        _topicSource = topicSource;
        _hostStartup = hostRunner;
        _fixture = fixture;
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task HandlingOnTimeout(bool enableAutoCommit)
    {
        const int messageCount = 100;
        var batchTriggerTimeout = TimeSpan.FromSeconds(1);
        var (topic, messages) = await _topicSource.CreateTopicWithMessages<BlackMessage>(_fixture, messageCount);
        var consumerSettings = _config.ToSettings(topic, enableAutoCommit);

        await using var host = await _hostStartup
            .CreateServiceCollection()
            .AddSubscriptions((s, _) =>
            {
                s.AddBatch(
                    consumerSettings,
                    new BatchConfiguration
                    {
                        BatchTriggerTimeout = batchTriggerTimeout,
                        MaxBatchSize = messageCount * 1000,
                        MaxBufferSize = messageCount * 1000
                    },
                    new JsonMessageDeserializer<BlackMessage>());
            })
            .RunHost();

        var messageHandler = host.GetHandler();

        await host.WhenAll(messageHandler.BlackSet.WaitUntil(messageCount, batchTriggerTimeout * 4));

        messageHandler.Black.Should().HaveCount(messageCount);

        await Task.Delay(consumerSettings.Config.AutoCommitIntervalMs ?? 0);

        _topicSource.GetLag(topic, consumerSettings.Config.GroupId).Should().OnlyContain(l => l.lag == 0);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task HandlingOnBatchSize(bool enableAutoCommit)
    {
        const int messageCount = 100;
        var batchTriggerTimeout = TimeSpan.FromSeconds(10);
        var (topic, messages) = await _topicSource.CreateTopicWithMessages<BlackMessage>(_fixture, messageCount);
        var consumerSettings = _config.ToSettings(topic, enableAutoCommit);

        await using var host = await _hostStartup
            .CreateServiceCollection()
            .AddSubscriptions((s, _) =>
            {
                s.AddBatch(
                    consumerSettings,
                    new BatchConfiguration
                    {
                        BatchTriggerTimeout = batchTriggerTimeout,
                        MaxBatchSize = messageCount,
                    },
                    new JsonMessageDeserializer<BlackMessage>());
            })
            .RunHost();

        var messageHandler = host.GetHandler();

        await host.WhenAll(messageHandler.BlackSet.WaitUntil(messageCount, batchTriggerTimeout / 2));

        messageHandler.Black.Should().HaveCount(messageCount);
        
        await Task.Delay(consumerSettings.Config.AutoCommitIntervalMs ?? 0);

        _topicSource.GetLag(topic, consumerSettings.Config.GroupId).Should().OnlyContain(l => l.lag == 0);
    }

    public Task InitializeAsync()
        => Task.CompletedTask;

    public async Task DisposeAsync()
        => await _topicSource.DisposeAsync();
}