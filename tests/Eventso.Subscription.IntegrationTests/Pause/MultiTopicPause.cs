using Eventso.Subscription.Hosting;
using Eventso.Subscription.Kafka;

namespace Eventso.Subscription.IntegrationTests.Pause;

public sealed class MultiTopicPause : IAsyncLifetime
{
    private readonly KafkaConfig _config;
    private readonly TopicSource _topicSource;
    private readonly TestHostStartup _hostStartup;
    private readonly IFixture _fixture;

    public MultiTopicPause(
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
    public async Task FailForever()
    {
        const int messageCount = 100;
        var topics = await _topicSource.CreateTopics(_fixture, messageCount);

        var consumerSettings = _config.ToSettings();

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

        messageHandler.RedSet.FailOnAny(int.MaxValue);

        using var diagnosticCollector = new DiagnosticCollector();

        await host.Start();

        await host.WhenAll(
            messageHandler.GreenSet.WaitUntil(messageCount),
            messageHandler.BlueSet.WaitUntil(messageCount),
            messageHandler.BlackSet.WaitUntil(messageCount));

        await Task.Delay(100);
        //we already have consumed Red topic here

        var newTopicsMessages = _topicSource.PublishNewMessages(topics, _fixture, messageCount);

        await host.WhenAll(
            messageHandler.GreenSet.WaitUntil(messageCount * 2),
            messageHandler.BlueSet.WaitUntil(messageCount * 2),
            messageHandler.BlackSet.WaitUntil(messageCount * 2));


        messageHandler.RedSet.Should().HaveCount(0);
        messageHandler.GreenSet.Should().HaveCount(messageCount * 2);
        messageHandler.BlueSet.Should().HaveCount(messageCount * 2);
        messageHandler.BlackSet.Should().HaveCount(messageCount * 2);

        var pausedActivity = diagnosticCollector.GetStarted(KafkaDiagnostic.Pause).Single();

        pausedActivity.GetTagItem("topic").Should().Be(topics.Red.Topic);

        await Task.Delay(consumerSettings.Config.AutoCommitIntervalMs + 500 ?? 0);

        topics.GetAll().Except(new[] { topics.Red.Topic }).SelectMany(t =>
                _topicSource.GetLag(t, consumerSettings.Config.GroupId))
            .Should().OnlyContain(x => x.lag == 0);
    }

    [Fact]
    public async Task ProcessingRestoredAfterRetries_Single()
    {
        const int messageCount = 100;
        var topics = await _topicSource.CreateTopics(_fixture, messageCount);

        var consumerSettings = _config.ToSettings();

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

        messageHandler.RedSet.FailOnAny(int.MaxValue);

        using var diagnosticCollector = new DiagnosticCollector();

        await host.Start();

        await host.WhenAll(
            messageHandler.GreenSet.WaitUntil(messageCount),
            messageHandler.BlueSet.WaitUntil(messageCount),
            messageHandler.BlackSet.WaitUntil(messageCount));

        await Task.Delay(100);
        //we already have consumed Red topic here

        await Task.Delay(TimeSpan.FromMilliseconds(consumerSettings.Config.SessionTimeoutMs ?? 45000));

        var pausedActivity = diagnosticCollector.GetStarted(KafkaDiagnostic.Pause).Single();
        pausedActivity.GetTagItem("topic").Should().Be(topics.Red.Topic);

        messageHandler.RedSet.ClearFails();
        //resume consume

        await host.WhenAll(
            messageHandler.RedSet.WaitUntil(messageCount));

        var resumedActivity = diagnosticCollector.GetStopped(KafkaDiagnostic.Pause).Single();
        resumedActivity.GetTagItem("topic").Should().Be(topics.Red.Topic);

        messageHandler.RedSet.Should().HaveCount(messageCount);
        messageHandler.GreenSet.Should().HaveCount(messageCount);
        messageHandler.BlueSet.Should().HaveCount(messageCount);
        messageHandler.BlackSet.Should().HaveCount(messageCount);

        await Task.Delay(consumerSettings.Config.AutoCommitIntervalMs + 500 ?? 0);

        topics.GetAll().SelectMany(t =>
                _topicSource.GetLag(t, consumerSettings.Config.GroupId))
            .Should().OnlyContain(x => x.lag == 0);
    }

    [Fact]
    public async Task ProcessingRestoredAfterRetries_Batch()
    {
        const int messageCount = 100;
        var topics = await _topicSource.CreateTopics(_fixture, messageCount);

        var consumerSettings = _config.ToSettings();

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

        messageHandler.BlueSet.FailOnAny(int.MaxValue);

        using var diagnosticCollector = new DiagnosticCollector();

        await host.Start();

        await host.WhenAll(
            messageHandler.RedSet.WaitUntil(messageCount),
            messageHandler.GreenSet.WaitUntil(messageCount),
            messageHandler.BlackSet.WaitUntil(messageCount));

        await Task.Delay(100);
        //we already have consumed Blue topic here

        await Task.Delay(TimeSpan.FromMilliseconds(consumerSettings.Config.SessionTimeoutMs ?? 45000));

        var pausedActivity = diagnosticCollector.GetStarted(KafkaDiagnostic.Pause).Single();
        pausedActivity.GetTagItem("topic").Should().Be(topics.Blue.Topic);

        messageHandler.BlueSet.ClearFails();
        //resume consume

        await host.WhenAll(
            messageHandler.BlueSet.WaitUntil(messageCount));

        var resumedActivity = diagnosticCollector.GetStopped(KafkaDiagnostic.Pause).Single();
        resumedActivity.GetTagItem("topic").Should().Be(topics.Blue.Topic);

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
    {
        await _topicSource.DisposeAsync();
    }
}