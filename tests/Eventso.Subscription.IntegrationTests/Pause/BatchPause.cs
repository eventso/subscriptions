using Eventso.Subscription.Hosting;
using Eventso.Subscription.Kafka;
using Eventso.Subscription.SpanJson;

namespace Eventso.Subscription.IntegrationTests.Pause;

public sealed class BatchPause : IAsyncLifetime
{
    private readonly KafkaConfig _config;
    private readonly TopicSource _topicSource;
    private readonly TestHostStartup _hostStartup;
    private readonly IFixture _fixture;

    public BatchPause(
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
    public async Task FailingBatch_Paused()
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
                        MaxBatchSize = messageCount / 10,
                        MaxBufferSize = messageCount
                    },
                    new JsonMessageDeserializer<BlackMessage>()))
            .CreateHost();

        using var diagnosticCollector = new DiagnosticCollector();

        var messageHandler = host.GetHandler();
        messageHandler.BlackSet.FailOnAny(int.MaxValue);

        await host.Start();

        await host.WhenAll(Task.Delay(TimeSpan.FromMilliseconds(consumerSettings.Config.SessionTimeoutMs ?? 45000) +
            batchTriggerTimeout * 2));


        var pausedActivity = diagnosticCollector.GetStarted(KafkaDiagnostic.Pause).Single();
        pausedActivity.GetTagItem("topic").Should().Be(topic);

        messageHandler.BlackSet.ClearFails();
        //resume consume

        await host.WhenAll(
            messageHandler.BlackSet.WaitUntil(messageCount));

        var resumedActivity = diagnosticCollector.GetStopped(KafkaDiagnostic.Pause).Single();
        resumedActivity.GetTagItem("topic").Should().Be(topic);

        messageHandler.BlackSet.Should().HaveCount(messageCount);

        await Task.Delay(consumerSettings.Config.AutoCommitIntervalMs ?? 500);

        _topicSource.GetLag(topic, consumerSettings.Config.GroupId)
            .Should().OnlyContain(x => x.lag == 0);
    }

    //[Fact]
    //public async Task VeryLongBatch_Paused()
    //{
    //    const int messageCount = 10_000;
    //    var batchTriggerTimeout = TimeSpan.FromSeconds(5);

    //    var (topic, messages) = await _topicSource.CreateTopicWithMessages<BlackMessage>(_fixture, messageCount);
    //    var consumerSettings = _config.ToSettings(topic);
    //    consumerSettings.Config.SessionTimeoutMs = 10000;

    //    await using var host = _hostStartup
    //        .CreateServiceCollection(delay: TimeSpan.FromSeconds(15))
    //        .AddSubscriptions((s, _) =>
    //            s.AddBatch(
    //                consumerSettings,
    //                new BatchConfiguration
    //                {
    //                    BatchTriggerTimeout = batchTriggerTimeout,
    //                    MaxBatchSize = messageCount / 100,
    //                    MaxBufferSize = messageCount
    //                },
    //                new JsonMessageDeserializer<BlackMessage>()))
    //        .CreateHost();

    //    using var diagnosticCollector = new DiagnosticCollector();

    //    var messageHandler = host.GetHandler();
        
    //    await host.Start();

    //    await host.WhenAll(messageHandler.BlackSet.WaitUntil(messageCount, TimeSpan.FromDays(1)));
    //}

    public Task InitializeAsync()
        => Task.CompletedTask;

    public async Task DisposeAsync()
        => await _topicSource.DisposeAsync();
}