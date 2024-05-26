using System.Runtime.InteropServices;
using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.ObjectPool;

namespace Eventso.Subscription.Kafka;

public sealed class KafkaGroupedMetadataProvider : IGroupedMetadataProvider<Event>
{
    private static readonly ObjectPool<StringBuilder> StringBuilderPool =
        new DefaultObjectPool<StringBuilder>(new StringBuilderPooledObjectPolicy());

    public static KafkaGroupedMetadataProvider Instance { get; } = new();

    private KafkaGroupedMetadataProvider()
    {
    }

    public KeyValuePair<string, object>[] GetFor(IEnumerable<Event> items)
    {
        var dict = new Dictionary<(string topic, Partition partition), PrettyOffsetRange>();

        foreach (var @event in items)
        {
            ref var range = ref CollectionsMarshal.GetValueRefOrAddDefault(dict, key: (@event.Topic, @event.Partition), out bool exists);
            if (!exists) range = new PrettyOffsetRange();
            range.Add(@event.Offset);
        }

        var sb = StringBuilderPool.Get();

        foreach (var (key, range) in dict)
        {
            range.Compact();

            if (sb.Length > 0) sb.Append(',');

            // rely on StringBuilder.AppendInterpolatedStringHandler.AppendFormatted + ISpanFormattable for PrettyOffsetRange
            sb.Append($"{key.topic}@{key.partition.Value}[{range}]");
        }

        KeyValuePair<string, object>[] result =
        [
            KeyValuePair.Create<string, object>("eventso.kafka.events", sb.ToString())
        ];
        StringBuilderPool.Return(sb);

        return result;
    }
}
