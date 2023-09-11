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

        await _topicSource.PublishMessages<WrongBlackMessage>(topics.Black.Topic, _fixture, messageCount);

        var consumerSettings = _config.ToSettings();
        consumerSettings.Config.SessionTimeoutMs = 15000;
        consumerSettings.Config.MaxPollIntervalMs = 20000;

        await using var host = _hostStartup
            .CreateServiceCollection()
            .AddSubscriptions((s, _) =>
                s.AddMultiTopic(
                    consumerSettings,
                    c => c
                        .AddJson<RedMessage>(topics.Red.Topic, bufferSize: 1)
                        .AddJson<GreenMessage>(topics.Green.Topic, bufferSize: 1)
                        .AddBatchJson<BlueMessage>(topics.Blue.Topic)
                        .AddBatchJson<BlackMessage>(topics.Black.Topic)))
            .CreateHost();

        var messageHandler = host.GetHandler();

        using var diagnosticCollector = new DiagnosticCollector();

        await host.Start();

        await host.WhenAll(
            messageHandler.RedSet.WaitUntil(messageCount),
            messageHandler.GreenSet.WaitUntil(messageCount),
            messageHandler.BlueSet.WaitUntil(messageCount));

        await Task.Delay(consumerSettings.Config.MaxPollIntervalMs!.Value + 1000);
        //we already have consumed Black topic here 2 times

        var pausedActivities = diagnosticCollector.GetStarted(KafkaDiagnostic.Pause).ToArray();
        pausedActivities.Length.Should().BeGreaterOrEqualTo(2);

        pausedActivities.Should().AllSatisfy(a => a.GetTagItem("topic").Should().Be(topics.Black.Topic));

        messageHandler.BlackSet.Clear();

        await _topicSource.DeleteRecords(topics.Black.Topic);
        await _topicSource.PublishMessages<BlackMessage>(topics.Black.Topic, _fixture, messageCount);
        //resume consume
        
        await host.WhenAll(
            messageHandler.BlackSet.WaitUntil(messageCount));

        var resumedActivities = diagnosticCollector.GetStopped(KafkaDiagnostic.Pause);
        resumedActivities.Should().AllSatisfy(a => a.GetTagItem("topic").Should().Be(topics.Black.Topic));

        messageHandler.RedSet.Should().HaveCount(messageCount);
        messageHandler.GreenSet.Should().HaveCount(messageCount);
        messageHandler.BlueSet.Should().HaveCount(messageCount);
        messageHandler.BlackSet.Should().HaveCount(messageCount);

        await Task.Delay(consumerSettings.Config.AutoCommitIntervalMs + 500 ?? 0);

        topics.GetAll().SelectMany(t =>
                _topicSource.GetLag(t, consumerSettings.Config.GroupId))
            .Should().OnlyContain(x => x.lag == 0);
    }

    public Task InitializeAsync()
        => Task.CompletedTask;

    public async Task DisposeAsync()
        => await _topicSource.DisposeAsync();
}