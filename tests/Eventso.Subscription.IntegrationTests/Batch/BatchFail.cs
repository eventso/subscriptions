using Confluent.Kafka;
using Eventso.Subscription.Hosting;
using Eventso.Subscription.SpanJson;

namespace Eventso.Subscription.IntegrationTests.Batch;

public sealed class BatchFail : IAsyncLifetime
{
    private readonly KafkaConfig _config;
    private readonly TopicSource _topicSource;
    private readonly TestHostStartup _hostStartup;
    private readonly IFixture _fixture;

    public BatchFail(
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

    [Fact]
    public async Task FailingBatch_NoCommits()
    {
        const int messageCount = 100;
        var batchTriggerTimeout = TimeSpan.FromSeconds(1);
        var (topic, messages) = await _topicSource.CreateTopicWithMessages<BlackMessage>(_fixture, messageCount);
        var consumerSettings = _config.ToSettings(topic);

        await using var host = _hostStartup
            .CreateServiceCollection()
            .AddSubscriptions((s, _) =>
                s.AddBatch(
                    consumerSettings,
                    new BatchConfiguration
                    {
                        BatchTriggerTimeout = batchTriggerTimeout,
                        MaxBatchSize = messageCount,
                        MaxBufferSize = messageCount
                    },
                    new JsonMessageDeserializer<BlackMessage>()))
            .CreateHost();

        var messageHandler = host.GetHandler();
        messageHandler.BlackSet.FailOn(messages.GetByIndex(79), count: 2000);

        await host.Start();

        await host.WhenAll(Task.Delay(batchTriggerTimeout * 4));

        messageHandler.BlackSet.Should().HaveCountLessThan(messageCount);

        await Task.Delay(consumerSettings.Config.AutoCommitIntervalMs ?? 0);

        _topicSource.GetCommittedOffsets(topic, consumerSettings.Config.GroupId).Should()
            .OnlyContain(o => o.Offset == Offset.Unset);
    }

    public Task InitializeAsync()
        => Task.CompletedTask;

    public async Task DisposeAsync()
        => await _topicSource.DisposeAsync();
}