using Microsoft.Extensions.Logging;

namespace Eventso.Subscription.Kafka.DeadLetter;

static partial class PoisonEventQueueLog
{
    [LoggerMessage(
        EventId = 1000,
        Level = LogLevel.Information,
        Message = "Assigned to {GroupId}-{Topic}-{Partition}")]
    public static partial void PartitionAssign(
        this ILogger<PoisonEventQueue> logger, string groupId, string topic, int partition);

    [LoggerMessage(
        EventId = 1001,
        Level = LogLevel.Information,
        Message = "Acquired poison keys from {GroupId}-{Topic}-{Partition}")]
    public static partial void PartitionKeysAcquired(
        this ILogger<PoisonEventQueue> logger, string groupId, string topic, int partition);
    
    [LoggerMessage(
        EventId = 1002,
        Level = LogLevel.Information,
        Message = "Revoked from to {GroupId}-{Topic}-{Partition}")]
    public static partial void PartitionRevoke(
        this ILogger<PoisonEventQueue> logger, string groupId, string topic, int partition);
    
    [LoggerMessage(
        EventId = 1003,
        Level = LogLevel.Information,
        Message = "Enqueue message from {GroupId}-{Topic}-{Partition} with offset {Offset} and key {Key} because of {Reason}")]
    public static partial void Enqueue(
        this ILogger<PoisonEventQueue> logger,
        string groupId,
        string topic,
        int partition,
        long offset,
        Guid key,
        string reason);
    
    [LoggerMessage(
        EventId = 1004,
        Level = LogLevel.Information,
        Message = "Dequeue message from {GroupId}-{Topic}-{Partition} with offset {Offset} and key {Key}")]
    public static partial void Dequeue(
        this ILogger<PoisonEventQueue> logger,
        string groupId,
        string topic,
        int partition,
        long offset,
        Guid key);
}