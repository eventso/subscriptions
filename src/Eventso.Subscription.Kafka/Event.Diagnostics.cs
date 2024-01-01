using System.Runtime.InteropServices;
using Confluent.Kafka;

namespace Eventso.Subscription.Kafka;

public readonly partial struct Event : IGroupedMetadata<Event>
{
    public static IEnumerable<KeyValuePair<string, object>[]> GroupedMetadata(IEnumerable<Event> items)
    {
        var dict = new Dictionary<(string topic, Partition), PrettyOffsetRange>();

        foreach (var @event in items)
        {
            ref var range = ref CollectionsMarshal.GetValueRefOrAddDefault(dict, key: (@event.Topic, @event.Partition), out bool exists);
            if (!exists) range = new PrettyOffsetRange();
            range.Add(@event.Offset);
        }

        return Enumerate(dict);

        static IEnumerable<KeyValuePair<string, object>[]> Enumerate(
            Dictionary<(string topic, Partition partition), PrettyOffsetRange> dict
        )
        {
            foreach (var (key, range) in dict)
            {
                range.Compact();
                yield return new[]
                {
                    new KeyValuePair<string, object>("kafka.topic", key.topic),
                    new KeyValuePair<string, object>("kafka.partition", key.partition.Value.ToString()),
                    new KeyValuePair<string, object>("kafka.offsets", range.ToString())
                };
            }
        }
    }
}
