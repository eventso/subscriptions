using System;

namespace Eventso.Subscription.Hosting
{
    public sealed class DeadLetterQueueOptions
    {
        /// <summary>
        /// Interval between whole DLQ reprocessing job run.
        /// </summary>
        public TimeSpan ReprocessingJobInterval { get; set; } = TimeSpan.FromMinutes(1);

        /// <summary>
        /// Max size of DLQ for a topic. When exceeded - exception is thrown.
        /// </summary>
        public int MaxTopicQueueSize { get; set; } = 1000;

        /// <summary>
        /// Max retry attempts for an event. When exceeded - event is no longer a subject for retry.
        /// </summary>
        public int? MaxRetryAttemptCount { get; set; } = 10;

        /// <summary>
        /// Minimum interval between retries of concrete event processing.
        /// </summary>
        public TimeSpan? MinHandlingRetryInterval { get; set; } = TimeSpan.FromMinutes(10);

        /// <summary>
        /// Maximum allowed duration of a retry. When exceeded - one more retry could be executed.
        /// </summary>
        public TimeSpan? MaxRetryDuration { get; set; } = TimeSpan.FromHours(1);
    }
}