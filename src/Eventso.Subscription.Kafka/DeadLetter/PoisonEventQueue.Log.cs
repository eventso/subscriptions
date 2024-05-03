using Microsoft.Extensions.Logging;

namespace Eventso.Subscription.Kafka.DeadLetter;

static partial class PoisonEventQueueLog
{
    private const string MessagePrefix = $"[{nameof(Eventso)}][{nameof(PoisonEventQueue)}] ";
    
    [LoggerMessage(
        EventId = 1000,
        Level = LogLevel.Information,
        Message = MessagePrefix + "Assigned to {GroupId}-{Topic}-{Partition}")]
    public static partial void PartitionAssigne(
        this ILogger<PoisonEventQueue> logger, string groupId, string topic, int partition);
    
    [LoggerMessage(
        EventId = 1001,
        Level = LogLevel.Information,
        Message = MessagePrefix + "Revoked from to {GroupId}-{Topic}-{Partition}")]
    public static partial void PartitionRevoke(
        this ILogger<PoisonEventQueue> logger, string groupId, string topic, int partition);
    
    [LoggerMessage(
        EventId = 1002,
        Level = LogLevel.Information,
        Message = MessagePrefix + "Enqueue message from {GroupId}-{Topic}-{Partition} with offset {Offset} and key {Key} because of {Reason}")]
    public static partial void Enqueue(
        this ILogger<PoisonEventQueue> logger,
        string groupId,
        string topic,
        int partition,
        long offset,
        Guid key,
        string reason);
    
    [LoggerMessage(
        EventId = 1003,
        Level = LogLevel.Information,
        Message = MessagePrefix + "Dequeue message from {GroupId}-{Topic}-{Partition} with offset {Offset} and key {Key}")]
    public static partial void Dequeue(
        this ILogger<PoisonEventQueue> logger,
        string groupId,
        string topic,
        int partition,
        long offset,
        Guid key);
}