using Confluent.Kafka;
using Eventso.Subscription.Kafka;

namespace Eventso.Subscription.Tests;

public class KafkaEventGroupedMetadataTests
{
    [Fact]
    public void RegularCase()
    {
        var events = new Event[]
        {
            new Event(new ConsumeResult<Guid, ConsumedMessage> { Topic = "topic1", Partition = 1, Offset = 1 }),
            new Event(new ConsumeResult<Guid, ConsumedMessage> { Topic = "topic1", Partition = 1, Offset = 2 }),
            new Event(new ConsumeResult<Guid, ConsumedMessage> { Topic = "topic1", Partition = 1, Offset = 3 }),
            new Event(new ConsumeResult<Guid, ConsumedMessage> { Topic = "topic1", Partition = 2, Offset = 3 }),
            new Event(new ConsumeResult<Guid, ConsumedMessage> { Topic = "topic1", Partition = 2, Offset = 4 }),
            new Event(new ConsumeResult<Guid, ConsumedMessage> { Topic = "topic1", Partition = 2, Offset = 5 }),
            new Event(new ConsumeResult<Guid, ConsumedMessage> { Topic = "topic1", Partition = 2, Offset = 7 }),
            new Event(new ConsumeResult<Guid, ConsumedMessage> { Topic = "topic2", Partition = 53, Offset = 107 }),
            new Event(new ConsumeResult<Guid, ConsumedMessage> { Topic = "topic2", Partition = 53, Offset = 108 }),
            new Event(new ConsumeResult<Guid, ConsumedMessage> { Topic = "topic2", Partition = 53, Offset = 379 }),
            new Event(new ConsumeResult<Guid, ConsumedMessage> { Topic = "topic2", Partition = 53, Offset = 380 }),
        };

        var res = Event.GroupedMetadata(events).ToArray();

        res.Should().BeEquivalentTo(new[]
        {
            new[]
            {
                new KeyValuePair<string, object>("kafka.topic", "topic1"),
                new KeyValuePair<string, object>("kafka.partition", "1"),
                new KeyValuePair<string, object>("kafka.offsets", "1-3"),
            },
            new[]
            {
                new KeyValuePair<string, object>("kafka.topic", "topic1"),
                new KeyValuePair<string, object>("kafka.partition", "2"),
                new KeyValuePair<string, object>("kafka.offsets", "3-5,7"),
            },
            new[]
            {
                new KeyValuePair<string, object>("kafka.topic", "topic2"),
                new KeyValuePair<string, object>("kafka.partition", "53"),
                new KeyValuePair<string, object>("kafka.offsets", "107-108,379-380"),
            }
        });
    }
}
