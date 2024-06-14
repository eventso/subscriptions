namespace Eventso.Subscription.Hosting;

public sealed class TopicSubscriptionConfiguration
{
    private TopicSubscriptionConfiguration(
        string topic,
        IMessageDeserializer serializer,
        HandlerConfiguration? handlerConfig,
        bool skipUnknownMessages,
        int bufferSize)
    {
        if (string.IsNullOrWhiteSpace(topic))
            throw new ArgumentException("Topic name is not specified.");

        if (bufferSize < 0) throw new ArgumentOutOfRangeException(nameof(bufferSize));

        Topic = topic;
        Serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        SkipUnknownMessages = skipUnknownMessages;
        HandlerConfig = handlerConfig ?? new HandlerConfiguration();
        BufferSize = bufferSize;
    }

    public TopicSubscriptionConfiguration(
        string topic,
        IMessageDeserializer serializer,
        HandlerConfiguration? handlerConfig = default,
        DeferredAckConfiguration? deferredAckConfiguration = default,
        bool skipUnknownMessages = true,
        int bufferSize = 0)
        : this(
            topic,
            serializer,
            handlerConfig,
            skipUnknownMessages,
            bufferSize)
    {
        DeferredAckConfiguration = deferredAckConfiguration ?? DeferredAckConfiguration.Disabled;
    }

    public TopicSubscriptionConfiguration(
        string topic,
        BatchConfiguration batchConfiguration,
        IMessageDeserializer serializer,
        HandlerConfiguration? handlerConfig = default,
        bool skipUnknownMessages = true,
        int bufferSize = 0)
        : this(
            topic,
            serializer,
            handlerConfig,
            skipUnknownMessages,
            bufferSize)
    {
        batchConfiguration.Validate();

        BatchConfiguration = batchConfiguration;
        BatchProcessingRequired = true;
    }

    public bool BatchProcessingRequired { get; }

    public bool SkipUnknownMessages { get; }

    public string Topic { get; set; }

    public TimeSpan? ObservingDelay { get; init; }

    public IMessageDeserializer Serializer { get; }

    public HandlerConfiguration HandlerConfig { get; }

    public BatchConfiguration? BatchConfiguration { get; }

    public DeferredAckConfiguration? DeferredAckConfiguration { get; }

    public int BufferSize { get; }
}