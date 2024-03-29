﻿using Eventso.Subscription.Hosting;

namespace Eventso.Subscription.IntegrationTests.MultiTopic;

public sealed class RetryFlow : IAsyncLifetime
{
    private readonly KafkaConfig _config;
    private readonly TopicSource _topicSource;
    private readonly TestHostStartup _hostStartup;
    private readonly IFixture _fixture;

    public RetryFlow(
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
    public async Task MixedTypes()
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
                        .AddJson<RedMessage>(topics.Red.Topic, bufferSize: 100)
                        .AddJson<GreenMessage>(topics.Green.Topic, bufferSize: 10)
                        .AddBatchJson<BlueMessage>(topics.Blue.Topic)
                        .AddBatchJson<BlackMessage>(topics.Black.Topic)))
            .CreateHost();

        var messageHandler = host.GetHandler();

        messageHandler.RedSet.FailOn(topics.Red.Messages.GetByIndex(0, 3, 17, 39, 99));
        messageHandler.GreenSet.FailOn(topics.Green.Messages.GetByIndex(1, 15), count: 3);
        messageHandler.BlueSet.FailOn(topics.Blue.Messages.GetByIndex(25, 67), count: 3);
        messageHandler.BlackSet.FailOn(topics.Black.Messages.GetByIndex(15, 16), count: 3);

        using var exceptionsCollector = new DiagnosticCollector();

        await host.Start();

        await host.WhenAll(
            messageHandler.RedSet.WaitUntil(messageCount),
            messageHandler.GreenSet.WaitUntil(messageCount),
            messageHandler.BlueSet.WaitUntil(messageCount),
            messageHandler.BlackSet.WaitUntil(messageCount));

        messageHandler.RedSet.Should().HaveCount(messageCount);
        messageHandler.GreenSet.Should().HaveCount(messageCount);
        messageHandler.BlueSet.Should().HaveCount(messageCount);
        messageHandler.BlackSet.Should().HaveCount(messageCount);

        exceptionsCollector.HandlerExceptions.Should().HaveCount(5 + 2 * 3 + 2 * 3 + 2 * 3);

        await Task.Delay(consumerSettings.Config.AutoCommitIntervalMs +500 ?? 0);

        topics.GetAll().SelectMany(t =>
                _topicSource.GetLag(t, consumerSettings.Config.GroupId))
            .Should().OnlyContain(x => x.lag == 0);
    }

    [Fact]
    public async Task SingleTopic_Batch()
    {
        const int messageCount = 100;
        var (topic, messages) = await _topicSource.CreateTopicWithMessages<BlackMessage>(_fixture, messageCount);

        var consumerSettings = _config.ToSettings();

        await using var host = _hostStartup
            .CreateServiceCollection()
            .AddSubscriptions((s, _) =>
                s.AddMultiTopic(
                    consumerSettings,
                    c => c
                        .AddBatchJson<BlackMessage>(topic)))
            .CreateHost();

        var messageHandler = host.GetHandler();
        messageHandler.BlackSet.FailOn(messages.GetByIndex(12, 79), count: 2);

        using var exceptionsCollector = new DiagnosticCollector();

        await host.Start();

        await host.WhenAll(messageHandler.BlackSet.WaitUntil(messageCount));

        messageHandler.BlackSet.Should().HaveCount(messageCount);
        exceptionsCollector.HandlerExceptions.Should().HaveCount(2 * 2);

        await Task.Delay(consumerSettings.Config.AutoCommitIntervalMs ?? 0);

        _topicSource.GetLag(topic, consumerSettings.Config.GroupId)
            .Should().OnlyContain(x => x.lag == 0);
    }


    [Fact]
    public async Task SingleTopic_Single()
    {
        const int messageCount = 100;
        var (topic, messages) = await _topicSource.CreateTopicWithMessages<RedMessage>(_fixture, messageCount);
        var consumerSettings = _config.ToSettings();

        await using var host = _hostStartup
            .CreateServiceCollection()
            .AddSubscriptions((s, _) =>
                s.AddMultiTopic(
                    consumerSettings,
                    c => c
                        .AddJson<RedMessage>(topic, bufferSize: 20)))
            .CreateHost();

        var messageHandler = host.GetHandler();
        messageHandler.RedSet.FailOn(messages.GetByIndex(12, 79), count: 2);

        using var exceptionsCollector = new DiagnosticCollector();

        await host.Start();

        await host.WhenAll(messageHandler.RedSet.WaitUntil(messageCount));

        messageHandler.RedSet.Should().HaveCount(messageCount);
        exceptionsCollector.HandlerExceptions.Should().HaveCount(2 * 2);

        await Task.Delay(consumerSettings.Config.AutoCommitIntervalMs ?? 0);

        _topicSource.GetLag(topic, consumerSettings.Config.GroupId)
            .Should().OnlyContain(x => x.lag == 0);
    }

    public Task InitializeAsync()
        => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        await _topicSource.DisposeAsync();
    }
}