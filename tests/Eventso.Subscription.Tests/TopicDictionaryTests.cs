using Eventso.Subscription.Kafka;

namespace Eventso.Subscription.Tests;

public sealed class TopicDictionaryTests
{
    private readonly Fixture _fixture = new();

    [Fact]
    public void SameLengthStrings()
    {
        var items = _fixture.CreateMany<(string topic, int)>(1000).ToArray();

        var dict = new TopicDictionary<int>(items);

        foreach (var (topic, value) in items)
        {
            dict.Get(topic).Should().Be(value);
        }
    }

    [Fact]
    public void DiffLengthStrings()
    {
        var items = Enumerable.Range(0, 1000)
            .Select(i => (new string(_fixture.Create<char>(), i), i))
            .ToArray();

        var dict = new TopicDictionary<int>(items);

        foreach (var (topic, value) in items)
        {
            dict.Get(topic).Should().Be(value);
        }
    }
}