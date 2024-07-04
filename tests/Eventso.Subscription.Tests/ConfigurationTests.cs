using Confluent.Kafka;
using Eventso.Subscription.Kafka;

namespace Eventso.Subscription.Tests;

public sealed class ConfigurationTests
{
    [Fact]
    public void CloningConsumerSettings_Cloned()
    {
        var settings = new KafkaConsumerSettings("brokers", "groupId", TimeSpan.FromMinutes(1), AutoOffsetReset.Error)
        {
            PauseAfterObserveDelay = TimeSpan.FromMinutes(10),
            Config =
            {
                Acks = Acks.All,
                EnableAutoCommit = true,
                FetchMaxBytes = 100500
            },
        };

        var clone = settings.GetForInstance(1);

        clone.Should().BeEquivalentTo(settings, c => c.Excluding(c => c.Config));
        clone.PauseAfterObserveDelay.Should().Be(settings.PauseAfterObserveDelay);
        clone.Config.Acks.Should().Be(settings.Config.Acks);
        clone.Config.EnableAutoCommit.Should().Be(settings.Config.EnableAutoCommit);
        clone.Config.FetchMaxBytes.Should().Be(settings.Config.FetchMaxBytes);
    }
}