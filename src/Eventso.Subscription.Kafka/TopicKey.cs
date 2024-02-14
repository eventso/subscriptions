using Confluent.Kafka;

namespace Eventso.Subscription.Kafka;

internal readonly record struct TopicKey(string Topic, Partition Partition)
{
    public TopicKey(string topic) : this(topic, Partition.Any)
    {
    }
}