using System.Runtime.CompilerServices;
using Confluent.Kafka;
using Eventso.Subscription.Kafka.DeadLetter;
using Eventso.Subscription.Observing.DeadLetter;

namespace Eventso.Subscription.Hosting;

public sealed class DisabledDeadLetterQueue : IPoisonEventQueueFactory, IDeadLetterQueueScopeFactory
{
    public static DisabledDeadLetterQueue Instance { get; } = new();
    
    public IPoisonEventQueue Create(string groupId, string subscriptionId)
        => DisabledPoisonEventQueue.Instance;

    public IDeadLetterQueueScope<TEvent> Create<TEvent>(TEvent @event) where TEvent : IEvent
        => DisabledDeadLetterQueueScopeFactory<TEvent>.Instance;

    public IDeadLetterQueueScope<TEvent> Create<TEvent>(IReadOnlyCollection<TEvent> events) where TEvent : IEvent
        => DisabledDeadLetterQueueScopeFactory<TEvent>.Instance;

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

        public ValueTask<bool> Contains(TopicPartition topicPartition, Guid key, CancellationToken token)
            => ValueTask.FromResult(false);

        public Task Enqueue(ConsumeResult<byte[], byte[]> @event, DateTime failureTimestamp, string failureReason, CancellationToken token)
            => Task.FromResult(false);

        public Task Dequeue(ConsumeResult<byte[], byte[]> @event, CancellationToken token)
            => Task.CompletedTask;

        public async IAsyncEnumerable<ConsumeResult<byte[], byte[]>> Peek([EnumeratorCancellation] CancellationToken token)
        {
            await Task.CompletedTask;
            yield break;
        }
    }
    
    private sealed class DisabledDeadLetterQueueScopeFactory<TEvent> : IDeadLetterQueueScope<TEvent>
        where TEvent : IEvent
    {
        public static IDeadLetterQueueScope<TEvent> Instance { get; } = new DisabledDeadLetterQueueScopeFactory<TEvent>();

        public IReadOnlyCollection<PoisonEvent<TEvent>> GetPoisonEvents()
            => Array.Empty<PoisonEvent<TEvent>>();

        public void Dispose()
        {
        }
    }
}