using System;
using System.Collections.Generic;
using System.Linq;

namespace Eventso.Subscription.Kafka;

internal readonly struct TopicDictionary<T>
{
    private readonly (int topicLength, (string topic, T item)[])[] _items;

    public TopicDictionary(IEnumerable<(string, T)> items)
    {
        _items = items.GroupBy(i => i.Item1.Length)
            .Select(i => (i.Key, i.ToArray()))
            .ToArray();
    }

    public IEnumerable<T> Items
        => _items.SelectMany(x => x.Item2.Select(i => i.item));

    public T Get(string topic)
    {
        for (var bucketIndex = 0; bucketIndex < _items.Length; bucketIndex++)
        {
            ref readonly var lengthGroup = ref _items[bucketIndex];

            if (lengthGroup.topicLength != topic.Length)
                continue;

            if (lengthGroup.Item2.Length == 1)
                return lengthGroup.Item2[0].item;

            for (var itemIndex = 0; itemIndex < lengthGroup.Item2.Length; itemIndex++)
            {
                ref readonly var topicItem = ref lengthGroup.Item2[itemIndex];

                if (topicItem.topic.Equals(topic))
                    return topicItem.item;
            }
        }

        throw new InvalidOperationException($"Value not found for key '{topic}'.");
    }
}