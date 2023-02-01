using Confluent.Kafka;
using Eventso.Subscription.Kafka;

namespace Eventso.Subscription.Tests;

public sealed class ConsumerAdapterTests
{
    private readonly Fixture _fixture = new();

    public ConsumerAdapterTests()
    {
        _fixture.Customize(
            new AutoNSubstituteCustomization { ConfigureMembers = true });
    }

    [Fact]
    public void AcknowledgeMessages_LastOffsetsCommitted()
    {
        var acked = new List<TopicPartitionOffset>();
        var consumer = _fixture.Create<IConsumer<Guid, ConsumedMessage>>();
        consumer.WhenForAnyArgs(
                c => c.Commit(default(IEnumerable<TopicPartitionOffset>)))
            .Do(c => acked.AddRange(c.Arg<IEnumerable<TopicPartitionOffset>>()));

        var adapter = new ConsumerAdapter(
            new CancellationTokenSource(),
            consumer,
            autoCommitMode: false);

        const string topic = "SomeTopic";

        var messages = Enumerable.Range(0, 4)
            .SelectMany(partition =>
                Enumerable.Range(3, (partition + 5) * 2)
                    .Select(offset =>
                        _fixture.Build<ConsumeResult<Guid, ConsumedMessage>>()
                            .With(e => e.Partition, new Partition(partition))
                            .With(e => e.Offset, offset)
                            .With(e => e.Topic, topic)
                            .Without(e => e.TopicPartitionOffset)
                            .Create())
            ).Select(r => new Event(r))
            .ToArray();

        adapter.Acknowledge(messages);

        acked.Should().BeEquivalentTo(
            new TopicPartitionOffset[]
            {
                new(topic, new Partition(0), 13),
                new(topic, new Partition(1), 15),
                new(topic, new Partition(2), 17),
                new(topic, new Partition(3), 19)
            });
    }

    [Fact]
    public void AcknowledgeMixedPartitionMessages_LastOffsetsCommitted()
    {
        var acked = new List<TopicPartitionOffset>();
        var consumer = _fixture.Create<IConsumer<Guid, ConsumedMessage>>();
        consumer.WhenForAnyArgs(
                c => c.Commit(default(IEnumerable<TopicPartitionOffset>)))
            .Do(c => acked.AddRange(c.Arg<IEnumerable<TopicPartitionOffset>>()));

        var adapter = new ConsumerAdapter(
            new CancellationTokenSource(),
            consumer,
            autoCommitMode: false);

        const string topic = "SomeTopic";

        var messages = Enumerable.Range(0, 60)
            .Select(offset =>
                _fixture.Build<ConsumeResult<Guid, ConsumedMessage>>()
                    .With(e => e.Partition, new Partition(offset % 4))
                    .With(e => e.Offset, offset)
                    .With(e => e.Topic, topic)
                    .Without(e => e.TopicPartitionOffset)
                    .Create()
            ).Select(r => new Event(r))
            .ToArray();

        adapter.Acknowledge(messages);

        acked.Should().BeEquivalentTo(
            new TopicPartitionOffset[]
            {
                new(topic, new Partition(0), 57),
                new(topic, new Partition(1), 58),
                new(topic, new Partition(2), 59),
                new(topic, new Partition(3), 60)
            });
    }

    [Fact]
    public void AcknowledgeMixedPartitionMessages2_LastOffsetsCommitted()
    {
        var acked = new List<TopicPartitionOffset>();
        var consumer = _fixture.Create<IConsumer<Guid, ConsumedMessage>>();
        consumer.WhenForAnyArgs(
                c => c.Commit(default(IEnumerable<TopicPartitionOffset>)))
            .Do(c => acked.AddRange(c.Arg<IEnumerable<TopicPartitionOffset>>()));

        var adapter = new ConsumerAdapter(
            new CancellationTokenSource(),
            consumer,
            autoCommitMode: false);

        const string topic = "SomeTopic";

        var rnd = new Random();

        var messages = Enumerable.Range(0, 1024)
            .Select(offset =>
                _fixture.Build<ConsumeResult<Guid, ConsumedMessage>>()
                    .With(e => e.Partition, new Partition(rnd.Next(0, 4)))
                    .With(e => e.Offset, offset)
                    .With(e => e.Topic, topic)
                    .Without(e => e.TopicPartitionOffset)
                    .Create()
            ).Select(r => new Event(r))
            .ToArray();

        adapter.Acknowledge(messages);

        acked.Should().BeEquivalentTo(
            messages.GroupBy(m => m.Partition.Value)
                .Select(x =>
                {
                    var last = x.Last();
                    return new TopicPartitionOffset(
                        last.Topic, last.Partition, last.Offset + 1);
                })
        );
    }
}