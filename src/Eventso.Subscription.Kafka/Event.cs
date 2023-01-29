using Confluent.Kafka;

namespace Eventso.Subscription.Kafka;

public readonly struct Event : IEvent
{
    private readonly ConsumeResult<Guid, ConsumedMessage> _consumeResult;

    public Event(ConsumeResult<Guid, ConsumedMessage> consumeResult)
        => _consumeResult = consumeResult;

    public DeserializationStatus DeserializationResult
        => _consumeResult.Message.Value.Status;

    public string Topic
        => _consumeResult.Topic;

    public Partition Partition
        => _consumeResult.Partition;

    public Offset Offset
        => _consumeResult.Offset;

    public Guid GetKey()
        => _consumeResult.Message.Key;

    public object GetMessage()
        => _consumeResult.Message.Value.Message ?? throw new InvalidOperationException("Unknown message");

    public string GetIdentity()
        => $"{_consumeResult.Topic} [{_consumeResult.Partition}] @{_consumeResult.Offset}";

    public IReadOnlyCollection<KeyValuePair<string, object>> GetMetadata()
    {
        var offset = new KeyValuePair<string, object>("eventso_offset", GetIdentity());
        var key = new KeyValuePair<string, object>("eventso_key", GetKey());

        var metadata = _consumeResult.Message.Value.GetMetadata();

        if (metadata?.Count > 0)
        {
            var result = new List<KeyValuePair<string, object>>(metadata.Count + 2);
            result.AddRange(metadata);
            result.Add(offset);
            result.Add(key);

            return result;
        }

        var status = DeserializationResult;
        var resultPair = new KeyValuePair<string, object>("eventso_result", status);

        if (status == DeserializationStatus.Success && _consumeResult.Message.Value.Message != null)
        {
            var type = new KeyValuePair<string, object>(
                "eventso_type",
                _consumeResult.Message.Value.Message.GetType().Name);

            return new[] { offset, resultPair, type };
        }

        return new[] { offset, resultPair };
    }

    internal TopicPartitionOffset GetTopicPartitionOffset()
        => _consumeResult.TopicPartitionOffset;
}