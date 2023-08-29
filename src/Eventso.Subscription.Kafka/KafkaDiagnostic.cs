using System.Diagnostics;
using Confluent.Kafka;

namespace Eventso.Subscription.Kafka;

public static class KafkaDiagnostic
{
    public static readonly string Consume = "kafka.consume";
    public static readonly string Pause = "kafka.pause";

    public static Activity SetTags<TK, TV>(this Activity activity, ConsumeResult<TK, TV> result)
    {
        return activity.AddTag("topic", result.Topic)
            .AddTag("partition", result.Partition.Value)
            .AddTag("offset", result.Offset.Value);
    }
}