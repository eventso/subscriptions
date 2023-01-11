using Eventso.Subscription.Hosting;

namespace Eventso.Subscription.IntegrationTests.MultiTopic;

public sealed class SuccessFlow : IAsyncLifetime
{
    private readonly KafkaConfig _config;
    private readonly TopicSource _topicSource;
    private readonly TestHostStartup _hostStartup;
    private readonly IFixture _fixture;

    public SuccessFlow(
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
    public async Task SuccessFlow1()
    {
        const int messageCount = 100;
        var topicRed = await _topicSource.CreateTopicWithMessages<RedMessage>(_fixture, messageCount);
        var topicGreen = await _topicSource.CreateTopicWithMessages<GreenMessage>(_fixture, messageCount);
        var topicBlue = await _topicSource.CreateTopicWithMessages<BlueMessage>(_fixture, messageCount);

        var serviceCollection = _hostStartup.CreateServiceCollection();
        serviceCollection
            .AddSubscriptions((s, _) =>
                s.AddMultiTopic(
                    _config with {GroupInstanceId = Guid.NewGuid().ToString()},
                    c => c
                        .AddJson<RedMessage>(topicRed.topic)
                        .AddJson<GreenMessage>(topicGreen.topic, bufferSize: 0)
                        .AddBatchJson<BlueMessage>(topicBlue.topic)));

        await using var host = new TestHost(serviceCollection);
        await host.Start();

        var messageHandler = host.GetHandler();

        var waitConsuming = Task.WhenAll(
            messageHandler.Red.WaitUntil(messageCount),
            messageHandler.Green.WaitUntil(messageCount),
            messageHandler.Blue.WaitUntil(messageCount));

        await await Task.WhenAny(waitConsuming, host.FailedCompletion);

        messageHandler.Red.Should().HaveCount(messageCount);
        messageHandler.Green.Should().HaveCount(messageCount);
        messageHandler.Blue.Should().HaveCount(messageCount *2);
    }

    public Task InitializeAsync()
        => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        await _topicSource.DisposeAsync();
    }
}