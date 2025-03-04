using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Eventso.Subscription.Kafka.DeadLetter;

static partial class PoisonEventRetryingServiceLog
{
    [LoggerMessage(
        EventId = 2000,
        Level = LogLevel.Information,
        Message = "{TopicPartitionOffset} in group {GroupId} retry started")]
    public static partial void RetryStarted(
        this ILogger<PoisonEventRetryingService> logger,
        string groupId,
        TopicPartitionOffset topicPartitionOffset);
    
    [LoggerMessage(
        EventId = 2001,
        Level = LogLevel.Information,
        Message = "{TopicPartitionOffset} in group {GroupId} retry successful")]
    public static partial void RetrySuccessful(
        this ILogger<PoisonEventRetryingService> logger,
        string groupId,
        TopicPartitionOffset topicPartitionOffset);
    
    [LoggerMessage(
        EventId = 2002,
        Level = LogLevel.Warning,
        Message = "{TopicPartitionOffset} in group {GroupId} retry failed")]
    public static partial void RetryFailed(
        this ILogger<PoisonEventRetryingService> logger,
        Exception exception,
        string groupId,
        TopicPartitionOffset topicPartitionOffset);
}