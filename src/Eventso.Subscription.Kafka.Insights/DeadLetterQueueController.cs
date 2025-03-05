using Confluent.Kafka;
using Eventso.Subscription.Hosting;
using Eventso.Subscription.Kafka.DeadLetter;
using Microsoft.AspNetCore.Mvc;

namespace Eventso.Subscription.Kafka.Insights;

[Route("insights/kafka/dlq")]
[ApiController]
public sealed class DeadLetterQueueController(
    IEnumerable<ISubscriptionCollection> subscriptions,
    IPoisonEventQueueRetryingService queueRetryingService,
    IPoisonEventStore eventStore) : ControllerBase
{
    private readonly IEnumerable<SubscriptionConfiguration> _subscriptionConfigurations = subscriptions.SelectMany(i => i);

    [HttpGet("offsets")]
    public async Task<IReadOnlyCollection<GroupTopicPartitionOffset>> GetPoisonOffsets(
        string groupId,
        string topic,
        CancellationToken token)
    {
        return await eventStore.GetPoisonedOffsets(groupId, topic, token)
            .Select(c => new GroupTopicPartitionOffset(groupId, topic, c.Partition.Value, c.Offset.Value))
            .ToListAsync(token);
    }

    [HttpPost("event")]
    public async Task<IActionResult> GetPoisonEvent(GroupTopicPartitionOffset offset, CancellationToken token)
    {
        var configuration = _subscriptionConfigurations.SingleOrDefault(i => i.Contains(offset.Topic));
        if (configuration == null)
            throw new ArgumentException($"Subscription to '{offset.Topic} not found.'");

        var @event = await eventStore.GetEvent(
            offset.GroupId,
            new TopicPartitionOffset(offset.Topic, offset.Partition, offset.Offset),
            token);

        var headers = new Headers();
        foreach (var header in @event.Headers)
            headers.Add(header.Key, header.Data.ToArray());

        var key = new KeyGuidDeserializer().Deserialize(
            @event.Key.Span,
            @event.Key.IsEmpty,
            new SerializationContext(MessageComponentType.Key, offset.Topic, headers));

        var consumedMessage = new ValueDeserializer(
                configuration.GetByTopic(offset.Topic).Serializer,
                new MessageHandlersRegistry())
            .Deserialize(
                @event.Value.Span,
                @event.Value.IsEmpty,
                new SerializationContext(MessageComponentType.Value, offset.Topic, headers));
        
        return new JsonResult(
            new
            {
                Key = key,
                Value = consumedMessage.Message,
                Metadata = consumedMessage.GetMetadata(),
                @event.CreationTimestamp,
                @event.TotalFailureCount,
                @event.LastFailureTimestamp,
                @event.LastFailureReason,
                @event.LockTimestamp,
                @event.UpdateTimestamp
            });
    }

    [HttpPost("delete")]
    public Task Delete(GroupTopicPartitionOffset offset, CancellationToken token)
    {
        //TODO: Mark in db table and delete in bg service on retry
        // assuming that it will called really rare
        // keep in mind that it will remove even not partitions of current consumer (another dlq instance)
        return eventStore.RemoveEvent(
            offset.GroupId,
            new TopicPartitionOffset(offset.Topic, offset.Partition, offset.Offset),
            token);
    }

    [HttpPost("retry")]
    public Task Retry(CancellationToken token)
    {
        //TODO: move time in db
        // will run only on consumers from calling machine
        return queueRetryingService.Run(token);
    }

    public readonly record struct GroupTopicPartitionOffset(string GroupId, string Topic, int Partition, long Offset);
}