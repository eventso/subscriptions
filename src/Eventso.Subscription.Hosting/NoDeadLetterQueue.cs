using Confluent.Kafka;
using Eventso.Subscription.Kafka.DeadLetter.Store;
using Eventso.Subscription.Observing.DeadLetter;

namespace Eventso.Subscription.Hosting;

internal sealed class NoDeadLetterQueue : IDeadLetterQueue, IDeadLetterQueueScopeFactory, IPoisonEventStore
{
    public static readonly string ErrorMessage =
        $"Dead letter queue was not configured, please refer to {nameof(ServiceCollectionExtensions.AddSubscriptions)} method.";

    public static readonly NoDeadLetterQueue Instance = new();


    // TODO znake
    /*
Nosorogh on Jun 27, 2022
so if someone makes mistake in the dlq configuration, he will find out that something is wrong only when the first poison message arrives? it could happen in night time :)
I think NoDeadLetterQueue is pointless, we should throw an exception at startup in case of any inconsistencies in dlq configuration
     */
    public void Enqueue(DeadLetter message)
        => throw new InvalidOperationException(ErrorMessage);

    public void EnqueueRange(IEnumerable<DeadLetter> messages)
        => throw new InvalidOperationException(ErrorMessage);

    public Task<long> Count(string topic, CancellationToken token)
        => throw new InvalidOperationException(ErrorMessage);

    public IAsyncEnumerable<StoredPoisonEvent> AcquireEventsForRetrying(string topic, CancellationToken token)
        => throw new InvalidOperationException(ErrorMessage);

    public Task<bool> IsStreamStored(string topic, Guid key, CancellationToken token)
        => throw new InvalidOperationException(ErrorMessage);

    public IAsyncEnumerable<StreamId> GetStoredStreams(IReadOnlyCollection<StreamId> streamIds, CancellationToken token)
        => throw new InvalidOperationException(ErrorMessage);

    public Task Add(DateTime timestamp, IReadOnlyCollection<OpeningPoisonEvent> events, CancellationToken token)
        => throw new InvalidOperationException(ErrorMessage);

    public Task Add(DateTime timestamp, OpeningPoisonEvent @event, CancellationToken token)
        => throw new InvalidOperationException(ErrorMessage);

    public Task AddFailure(DateTime timestamp, OccuredFailure failure, CancellationToken token)
        => throw new InvalidOperationException(ErrorMessage);

    public Task AddFailures(DateTime timestamp, IReadOnlyCollection<OccuredFailure> failures, CancellationToken token)
        => throw new InvalidOperationException(ErrorMessage);

    public Task Remove(TopicPartitionOffset partitionOffset, CancellationToken token)
        => throw new InvalidOperationException(ErrorMessage);

    public Task Remove(IReadOnlyCollection<TopicPartitionOffset> partitionOffsets, CancellationToken token)
        => throw new InvalidOperationException(ErrorMessage);

    public IDeadLetterQueueScope<TEvent> Create<TEvent>(TEvent @event) where TEvent : IEvent
        => throw new InvalidOperationException(ErrorMessage);

    public IDeadLetterQueueScope<TEvent> Create<TEvent>(IReadOnlyCollection<TEvent> events) where TEvent : IEvent
        => throw new InvalidOperationException(ErrorMessage);
}