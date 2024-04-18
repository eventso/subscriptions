using System.Runtime.CompilerServices;
using Confluent.Kafka;
using Eventso.Subscription.Kafka.DeadLetter;

namespace Eventso.Subscription.Hosting;

public sealed class PoisonEventQueueFactory : IPoisonEventQueueFactory
{
    private readonly Dictionary<(string, string), PoisonEventQueue>? _poisonEventQueues;

    public PoisonEventQueueFactory(
        IEnumerable<ISubscriptionCollection> subscriptions,
        DeadLetterQueueOptions deadLetterQueueOptions,
        IPoisonEventStore poisonEventStore)
    {
        _poisonEventQueues = deadLetterQueueOptions.IsEnabled
            ? subscriptions
                .SelectMany(x => x)
                .SelectMany(c => c.ClonePerConsumerInstance())
                .ToDictionary(
                    c => (c.Settings.Config.GroupId, c.SubscriptionConfigurationId),
                    c => new PoisonEventQueue(
                        poisonEventStore,
                        c.Settings.Config.GroupId,
                        deadLetterQueueOptions.MaxTopicQueueSize))
            : null;

    }
    
    public IPoisonEventQueue Create(string groupId, string subscriptionId)
    {
        if (_poisonEventQueues == null)
            return DisabledPoisonEventQueue.Instance;
        
        return _poisonEventQueues.TryGetValue((groupId, subscriptionId), out var poisonEventQueue)
            ? poisonEventQueue
            : throw new Exception($"Unknown group {groupId} and subscription {subscriptionId}");
    }
    
    private sealed class DisabledPoisonEventQueue : IPoisonEventQueue
    {
        public static IPoisonEventQueue Instance { get; } = new DisabledPoisonEventQueue();

        public bool IsEnabled => false;

        public void Assign(TopicPartition topicPartition)
        {
        }

        public void Revoke(TopicPartition topicPartition)
        {
        }

        public ValueTask<bool> IsPoison(TopicPartition topicPartition, Guid key, CancellationToken token)
        {
            return ValueTask.FromResult(false);
        }

        public Task Blame(PoisonEvent @event, DateTime failureTimestamp, string failureReason, CancellationToken token)
        {
            return Task.FromResult(false);
        }

        public Task Rehabilitate(PoisonEvent @event, CancellationToken token)
        {
            return Task.CompletedTask;
        }

        public async IAsyncEnumerable<PoisonEvent> GetEventsForRetrying([EnumeratorCancellation] CancellationToken token)
        {
            await Task.CompletedTask;
            yield break;
        }
    }
}