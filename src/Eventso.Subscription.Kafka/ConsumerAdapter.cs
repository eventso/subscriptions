using Confluent.Kafka;

namespace Eventso.Subscription.Kafka;

public sealed class ConsumerAdapter : IConsumer<Event>
{
    private readonly CancellationTokenSource _cancellationTokenSource;
    private readonly IConsumer<Guid, ConsumedMessage> _consumer;

    public ConsumerAdapter(
        CancellationTokenSource cancellationTokenSource,
        IConsumer<Guid, ConsumedMessage> consumer)
    {
        _cancellationTokenSource = cancellationTokenSource;
        _consumer = consumer;
        Subscription = string.Join(',', _consumer.Subscription);
    }

    public CancellationToken CancellationToken => _cancellationTokenSource.Token;

    public string Subscription { get; }

    public void Acknowledge(in Event events)
    {
        var offset = new TopicPartitionOffset[]
        {
            new(events.Topic, events.Partition, events.Offset + 1)
        };

        _consumer.Commit(offset);
    }

    public void Acknowledge(IReadOnlyList<Event> events)
    {
        if (events.Count == 0)
            return;

        if (events.Count == 1)
        {
            Acknowledge(events[0]);
            return;
        }

        _consumer.Commit(GetLatestOffsets(events));

        static IEnumerable<TopicPartitionOffset> GetLatestOffsets(IReadOnlyList<Event> events)
        {
            var offsets = new Dictionary<(string, Partition), Offset>(4);

            for (var i = 0; i < events.Count; i++)
            {
                var current = events[i];
                var isLastMessage = i == events.Count - 1;

                if (isLastMessage || !EqualPartition(current, events[i + 1]))
                {
                    var key = (current.Topic, current.Partition);
                    offsets[key] = current.Offset;
                }
            }

            return offsets.Select(o => 
                new TopicPartitionOffset(o.Key.Item1, o.Key.Item2, o.Value + 1));

            static bool EqualPartition(Event left, Event right)
                => left.Partition == right.Partition &&
                   left.Topic.Equals(right.Topic);
        }
    }


    public void Cancel() => _cancellationTokenSource.Cancel();
}