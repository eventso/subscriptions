namespace Eventso.Subscription.Configurations;

/// <summary>
/// Defaults:
/// <see cref="MaxBufferSize"/> = 1000,
/// <see cref="Timeout"/> = 10 minutes.
/// </summary>
public sealed record DeferredAckConfiguration
{
    private readonly int _maxBufferSize = 1000;

    public static DeferredAckConfiguration Disabled { get; } = new()
    {
        Timeout = TimeSpan.Zero,
        MaxBufferSize = 0
    };

    /// <summary>
    /// Timeout before acknowledging deferred skipped messages.
    /// </summary>
    public TimeSpan Timeout { get; init; } = TimeSpan.FromMinutes(10);

    /// <summary>
    /// Max number of deferred skipped messages in buffer.
    /// </summary>
    public int MaxBufferSize
    {
        get => _maxBufferSize;
        init
        {
            if (value < 0)
                throw new ApplicationException(
                    $"Max batch size ({MaxBufferSize} should not be less than 0.");

            _maxBufferSize = value;
        }
    }
}