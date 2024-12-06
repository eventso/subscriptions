using System.Runtime.CompilerServices;
using Confluent.Kafka;
using Eventso.Subscription.Kafka;
using Eventso.Subscription.Kafka.DeadLetter;

namespace Eventso.Subscription.Hosting;

public sealed class DisabledDeadLetterQueue : IPoisonEventQueueFactory
{
    public static DisabledDeadLetterQueue Instance { get; } = new();
    
    public IPoisonEventQueue Create(string groupId, string subscriptionId)
        => DisabledPoisonEventQueue.Instance;

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

        public Task<IKeySet<Event>> GetKeys(string topic, CancellationToken token)
        {
            return Task.FromResult((IKeySet<Event>)new EmptyKeySet());
        }

        public Task Add(ConsumeResult<byte[], byte[]> @event, DateTime failureTimestamp, string failureReason, CancellationToken token)
            => Task.FromResult(false);

        public Task Enqueue(ConsumeResult<byte[], byte[]> @event, DateTime failureTimestamp, string failureReason, CancellationToken token)
            => Task.FromResult(false);

        public Task Dequeue(ConsumeResult<byte[], byte[]> @event, CancellationToken token)
            => Task.CompletedTask;

        public IAsyncEnumerable<ConsumeResult<byte[], byte[]>> Peek(CancellationToken token)
            => AsyncEnumerable.Empty<ConsumeResult<byte[], byte[]>>();

        private sealed class EmptyKeySet : IKeySet<Event>
        {
            public bool Contains(in Event item) => false;

            public bool IsEmpty() => true;
        }
    }
}