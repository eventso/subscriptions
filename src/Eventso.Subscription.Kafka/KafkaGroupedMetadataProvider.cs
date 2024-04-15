using System.Runtime.InteropServices;
using Confluent.Kafka;

namespace Eventso.Subscription.Kafka;

public sealed class KafkaGroupedMetadataProvider : IGroupedMetadataProvider<Event>
{
    public static KafkaGroupedMetadataProvider Instance { get; } = new();

    private KafkaGroupedMetadataProvider()
    {
    }

    public List<Dictionary<string, object>> GetFor(IEnumerable<Event> items)
    {
        var dict = new Dictionary<(string topic, Partition partition), PrettyOffsetRange>();

        foreach (var @event in items)
        {
            ref var range = ref CollectionsMarshal.GetValueRefOrAddDefault(dict, key: (@event.Topic, @event.Partition), out bool exists);
            if (!exists) range = new PrettyOffsetRange();
            range.Add(@event.Offset);
        }

        var result = new List<Dictionary<string, object>>(capacity: dict.Count);

        foreach (var (key, range) in dict)
        {
            range.Compact();

            result.Add(new Dictionary<string, object>(capacity: 3)
            {
                ["kafka.topic"] = key.topic,
                ["kafka.partition"] = key.partition.Value.ToString(),
                ["kafka.offsets"] = range.ToString()
            });
        }

        return result;
    }
}
