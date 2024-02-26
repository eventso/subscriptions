using Confluent.Kafka;
using Eventso.Subscription.Hosting;
using Eventso.Subscription.Kafka;
using Eventso.Subscription.SpanJson;

namespace Eventso.Subscription.IntegrationTests.Serialization;

public sealed class SerializationFail : IAsyncLifetime
{
    private readonly KafkaConfig _config;
    private readonly TopicSource _topicSource;
    private readonly TestHostStartup _hostStartup;
    private readonly IFixture _fixture;

    public SerializationFail(
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
        var (topic, messages) = await _topicSource.CreateTopicWithMessages<WrongBlackMessage>(_fixture, messageCount);
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

        using var diagnosticCollector = new DiagnosticCollector();

        await host.Start();

        await host.WhenAll(Task.Delay(batchTriggerTimeout * 4));

        messageHandler.BlackSet.Should().HaveCount(0);

        await Task.Delay(consumerSettings.Config.AutoCommitIntervalMs ?? 0);

        _topicSource.GetCommittedOffsets(topic, consumerSettings.Config.GroupId).Should()
            .OnlyContain(o => o.Offset == Offset.Unset);

        var pausedActivity = diagnosticCollector.GetStarted(KafkaDiagnostic.Pause).Single();

        pausedActivity.GetTagItem("topic").Should().Be(topic);
    }

    [Fact]
    public async Task ProcessingRestoredAfterDeadMessageGone_Batch()
    {
        const int messageCount = 100;
        var topics = await _topicSource.CreateTopics(_fixture, messageCount);

        var consumerSettings = _config.ToSettings(pauseAfter: TimeSpan.FromSeconds(15));
        consumerSettings.Config.MaxPollIntervalMs = 20000;
        consumerSettings.Config.SessionTimeoutMs = 15000;

        await using var host = _hostStartup
            .CreateServiceCollection()
            .AddSubscriptions((s, _) =>
                s.AddMultiTopic(
                    consumerSettings,
                    c => c
                        .AddJson<RedMessage>(topics.Red.Topic, bufferSize: 1)
                        .AddJson<GreenMessage>(topics.Green.Topic, bufferSize: 1)
                        .AddBatchJson<BlueMessage>(topics.Blue.Topic)
                        .AddBatchJson<WrongBlackMessage>(topics.Black.Topic)))
            .CreateHost();

        var messageHandler = host.GetHandler();

        using var diagnosticCollector = new DiagnosticCollector();

        await host.Start();

        await Task.WhenAll(
            messageHandler.RedSet.WaitUntil(messageCount),
            messageHandler.GreenSet.WaitUntil(messageCount),
            messageHandler.BlueSet.WaitUntil(messageCount));

        await Task.Delay(consumerSettings.Config.MaxPollIntervalMs!.Value * 2 + 1000);

        //no commits
        _topicSource.GetCommittedOffsets(topics.Black.Topic, consumerSettings.Config.GroupId)
            .Should().OnlyContain(x => x.Offset.Equals(Offset.Unset));

        var pausedActivities = diagnosticCollector.GetStarted(KafkaDiagnostic.Pause).ToArray();
        pausedActivities.Length.Should().BeGreaterOrEqualTo(1);

        pausedActivities.Should().AllSatisfy(a => a.GetTagItem("topic").Should().Be(topics.Black.Topic));
    }

    public Task InitializeAsync()
        => Task.CompletedTask;

    public async Task DisposeAsync()
        => await _topicSource.DisposeAsync();
}