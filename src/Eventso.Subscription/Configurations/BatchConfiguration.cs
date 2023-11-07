namespace Eventso.Subscription.Configurations;

/// <summary>
/// Defaults:
/// <see cref="MaxBatchSize"/> = 1000,
/// <see cref="MaxBufferSize"/> = <see cref="MaxBatchSize"/> * 3,
/// <see cref="BatchTriggerTimeout"/> = 1 minute,
/// <see cref="HandlingStrategy"/> = <see cref="BatchHandlingStrategy.OrderedWithinKey"/>. 
/// </summary>
public sealed record BatchConfiguration
{
    /// <summary>
    /// Max number of anticipated messages in one batch. Default: 1000
    /// </summary>
    public int MaxBatchSize { get; init; } = 1000;

    /// <summary>
    /// Max number of anticipated and skipped messages in buffer. Default: 3 * MaxBatchSize
    /// </summary>
    public int MaxBufferSize { get; init; }

    /// <summary>
    /// Timeout before firing incomplete batch of anticipated messages. Default: 1min
    /// </summary>
    public TimeSpan BatchTriggerTimeout { get; init; } = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Batch handling strategy. Default: strong ordering guarantee
    /// </summary>
    public BatchHandlingStrategy HandlingStrategy { get; init; } = BatchHandlingStrategy.OrderedWithinKey;

    internal int GetMaxBufferSize()
        => MaxBufferSize != 0 ? MaxBufferSize : MaxBatchSize * 3;

    public void Validate()
    {
        if (MaxBatchSize < 1)
            throw new ApplicationException(
                $"Max batch size {MaxBatchSize} should not be less than 1.");

        if (MaxBufferSize != default && MaxBufferSize < MaxBatchSize)
            throw new ApplicationException(
                $"Max buffer size {MaxBufferSize} should not be less than max batch size {MaxBatchSize}.");
    }
}