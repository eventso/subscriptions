namespace Eventso.Subscription.Kafka.DeadLetter;

public sealed record DeadLetterQueueOptions
{
    /// <summary>
    /// Interval between whole DLQ reprocessing job run.
    /// </summary>
    public TimeSpan ReprocessingJobInterval { get; init; } = TimeSpan.FromMinutes(60);

    /// <summary>
    /// Max size of DLQ for a topic. When exceeded - exception is thrown.
    /// </summary>
    public int MaxTopicQueueSize { get; init; } = 1000;

    /// <summary>
    /// Max retry attempts for an event. When exceeded - event is no longer a subject for retry.
    /// </summary>
    public int MaxRetryAttemptCount { get; init; } = 10;

    /// <summary>
    /// Minimum interval between retries of concrete event processing.
    /// </summary>
    public TimeSpan MinHandlingRetryInterval { get; init; } = TimeSpan.FromMinutes(10);

    /// <summary>
    /// Maximum allowed duration of a retry. When exceeded - one more retry could be executed.
    /// </summary>
    public TimeSpan MaxRetryDuration { get; init; } = TimeSpan.FromHours(1);

    internal RetrySchedulingOptions GetRetrySchedulingOptions()
        => new(MaxRetryAttemptCount, MinHandlingRetryInterval, MaxRetryDuration);

    public sealed record RetrySchedulingOptions(
        int MaxRetryAttemptCount,
        TimeSpan MinHandlingRetryInterval,
        TimeSpan MaxRetryDuration);
}