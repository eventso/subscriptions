namespace Eventso.Subscription.Hosting;

public sealed class TopicSubscriptionConfiguration
{
    public TopicSubscriptionConfiguration(
        string topic,
        IMessageDeserializer serializer,
        HandlerConfiguration? handlerConfig = default,
        bool skipUnknownMessages = true,
        int bufferSize = 0)
    {
        if (string.IsNullOrWhiteSpace(topic))
            throw new ArgumentException("Topic name is not specified.");

        if (bufferSize < 0) throw new ArgumentOutOfRangeException(nameof(bufferSize));

        Topic = topic;
        Serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        HandlerConfig = handlerConfig ?? new HandlerConfiguration();
        SkipUnknownMessages = skipUnknownMessages;
        BufferSize = bufferSize;
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

    public int BufferSize { get; }
}