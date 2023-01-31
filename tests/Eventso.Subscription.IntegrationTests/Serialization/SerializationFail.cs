using Confluent.Kafka;
using Eventso.Subscription.Hosting;
using Eventso.Subscription.SpanJson;
using FluentAssertions;

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

        await host.Start();

        var act = () => host.WhenAll(Task.Delay(batchTriggerTimeout * 4));

        await act.Should().ThrowAsync<ConsumeException>()
            .WithInnerExceptionExactly<ConsumeException, InvalidEventException>();

        messageHandler.BlackSet.Should().HaveCount(0);

        _topicSource.GetCommittedOffsets(topic, consumerSettings.Config.GroupId).Should()
            .OnlyContain(o => o.Offset == Offset.Unset);

    }

    public Task InitializeAsync()
        => Task.CompletedTask;

    public async Task DisposeAsync()
        => await _topicSource.DisposeAsync();
}