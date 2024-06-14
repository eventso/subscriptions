namespace Eventso.Subscription.Observing.DeadLetter;

public static partial class PoisonEventHandlerLog
{
    public static void UnwrapStarted<TEvent>(
        this ILogger<PoisonEventHandler<TEvent>> logger, int eventsCount) where TEvent : IEvent
        => logger.UnwrapStartedLog(eventsCount);

    public static void UnwrapInProgress<TEvent>(
        this ILogger<PoisonEventHandler<TEvent>> logger, int unwrappedCount, int eventsCount) where TEvent : IEvent
        => logger.UnwrapInProgressLog(unwrappedCount, eventsCount);

    public static void UnwrapCompleted<TEvent>(
        this ILogger<PoisonEventHandler<TEvent>> logger, int eventsCount) where TEvent : IEvent
        => logger.UnwrapCompletedLog(eventsCount);

    [LoggerMessage(
        EventId = 3000,
        Level = LogLevel.Information,
        Message = "Unwrap started for {EventsCount} events to find poison")]
    static partial void UnwrapStartedLog(this ILogger logger, int eventsCount);

    [LoggerMessage(
        EventId = 3001,
        Level = LogLevel.Information,
        Message = "Unwrap processed {UnwrappedCount} of {EventsCount} events to find poison")]
    static partial void UnwrapInProgressLog(this ILogger logger, int unwrappedCount, int eventsCount);

    [LoggerMessage(
        EventId = 3002,
        Level = LogLevel.Information,
        Message = "Unwrap completed for {EventsCount} events to find poison")]
    static partial void UnwrapCompletedLog(this ILogger logger, int eventsCount);
}