using Confluent.Kafka;
using Eventso.Subscription.Hosting;

namespace Eventso.Subscription.IntegrationTests.MultiTopic;

public sealed class FailFlow : IAsyncLifetime
{
    private readonly KafkaConfig _config;
    private readonly TopicSource _topicSource;
    private readonly TestHostStartup _hostStartup;
    private readonly IFixture _fixture;

    public FailFlow(
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
    public async Task FailingSingleWithAllInBuffer()
    {
        const int messageCount = 100;
        _fixture.Inject(42); // fix key => same partition for all

        var topics = await _topicSource.CreateTopics(_fixture, messageCount);
        var consumerSettings = _config.ToSettings();

        await using var host = _hostStartup
            .CreateServiceCollection()
            .AddSubscriptions((s, _) =>
                s.AddMultiTopic(
                    consumerSettings,
                    c => c
                        .AddJson<RedMessage>(topics.Red.Topic, bufferSize: messageCount)
                        .AddJson<GreenMessage>(topics.Green.Topic, bufferSize: 10)
                        .AddBatchJson<BlueMessage>(topics.Blue.Topic)
                        .AddBatchJson<BlackMessage>(topics.Black.Topic)))
            .CreateHost();

        var messageHandler = host.GetHandler();
        messageHandler.RedSet.FailOn(topics.Red.Messages.GetByIndex(13), count: 2000);

        await host.Start();

        await host.WhenAll(
            messageHandler.GreenSet.WaitUntil(messageCount),
            messageHandler.BlueSet.WaitUntil(messageCount),
            messageHandler.BlackSet.WaitUntil(messageCount));

        await Task.Delay(TimeSpan.FromSeconds(5));

        messageHandler.RedSet.Should().HaveCountLessThan(messageCount);

        topics.GetAll()
            .Where(x => x != topics.Red.Topic)
            .SelectMany(t =>
                _topicSource.GetLag(t, consumerSettings.Config.GroupId))
            .Should().OnlyContain(x => x.lag == 0);

        await Task.Delay(consumerSettings.Config.AutoCommitIntervalMs ?? 0);

        _topicSource
            .GetCommittedOffsets(topics.Red.Topic, consumerSettings.Config.GroupId)
            .Sum(o => o.Offset == Offset.Unset ? 0 : o.Offset.Value)
            .Should().Be(13);
    }

    public Task InitializeAsync()
        => Task.CompletedTask;

    public async Task DisposeAsync()
        => await _topicSource.DisposeAsync();
}