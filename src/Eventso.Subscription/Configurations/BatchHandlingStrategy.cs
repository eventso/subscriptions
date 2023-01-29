namespace Eventso.Subscription.Configurations;

public enum BatchHandlingStrategy
{
    /// <summary>
    /// All messages of batch have the same type. The most efficient strategy.
    /// </summary>
    SingleType,

    /// <summary>
    /// All messages of batch have the same type.
    /// Only the last one of messages with same key will be handled.
    /// Note: It's not intended for use with keys greater than 16 byte.
    /// </summary>
    SingleTypeLastByKey,

    /// <summary>
    /// Total order of messages is preserved. The most inefficient strategy.
    /// </summary>
    OrderedWithinKey,

    /// <summary>
    /// Order of messages is preserved within the same type but not within the same key.
    /// </summary>
    OrderedWithinType
}