using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Eventso.Subscription.Kafka;

internal static partial class KafkaConsumerLog
{
    [LoggerMessage(
        EventId = 4000,
        Level = LogLevel.Error,
        Message = "KafkaConsumer internal error: Topics: {Topics}, {Reason}, Fatal={IsFatal}, IsLocal= {IsLocal}, IsBroker={IsBroker}")]
    public static partial void ConsumeError(
        this ILogger<KafkaConsumer> logger,
        IReadOnlyCollection<string> topics,
        string reason,
        bool isFatal,
        bool isLocal,
        bool isBroker);
    
    [LoggerMessage(
        EventId = 4001,
        Level = LogLevel.Error,
        Message = "Serialization exception for message {TopicPartitionOffset}, consuming paused")]
    public static partial void SerializationError(
        this ILogger<KafkaConsumer> logger,
        Exception exception,
        TopicPartitionOffset? topicPartitionOffset);
    
    [LoggerMessage(
        EventId = 4002,
        Level = LogLevel.Information,
        Message = "Waiting paused task for topic {Topic}")]
    public static partial void WaitingPaused(this ILogger<KafkaConsumer> logger, string topic);
    
    [LoggerMessage(
        EventId = 4003,
        Level = LogLevel.Information,
        Message = "Topic '{Topic}' consuming paused. Partitions: {Partitions}")]
    public static partial void ConsumePaused(this ILogger<KafkaConsumer> logger, string topic, string partitions);
    
    [LoggerMessage(
        EventId = 4004,
        Level = LogLevel.Information,
        Message = "Topic '{Topic}' consuming resumed. Partitions: {Partitions}")]
    public static partial void ConsumeResumed(this ILogger<KafkaConsumer> logger, string topic, string partitions);
    
    [LoggerMessage(
        EventId = 4005,
        Level = LogLevel.Information,
        Message = "Consumer rebalance. Assigned: {TopicPartitions}")]
    public static partial void RebalancePartitionsAssigned(
        this ILogger<KafkaConsumer> logger,
        IReadOnlyCollection<TopicPartition> topicPartitions);
    
    [LoggerMessage(
        EventId = 4006,
        Level = LogLevel.Information,
        Message = "Consumer rebalance. Revoked: {TopicPartitionOffsets}")]
    public static partial void RebalancePartitionsRevoked(
        this ILogger<KafkaConsumer> logger,
        IReadOnlyCollection<TopicPartitionOffset> topicPartitionOffsets);
    
    [LoggerMessage(
        EventId = 4007,
        Level = LogLevel.Information,
        Message = "Consumer closed. Assignment: {topicPartitions}")]
    public static partial void ConsumerClosed(
        this ILogger<KafkaConsumer> logger,
        IReadOnlyCollection<TopicPartition> topicPartitions);
}