using Eventso.Subscription.Configurations;

namespace Eventso.Subscription.Http.Hosting;

public sealed class SubscriptionConfiguration
{
    public SubscriptionConfiguration(string topic, IMessageDeserializer deserializer)
    {
        Topic = string.IsNullOrWhiteSpace(topic)
            ? throw new ArgumentException("Topic name is not specified.")
            : topic;

        Deserializer = deserializer ?? throw new ArgumentNullException(nameof(deserializer));
    }

    public SubscriptionConfiguration(
        string topic,
        IMessageDeserializer deserializer,
        BatchConfiguration batchConfiguration)
        : this(topic, deserializer)
    {
        batchConfiguration.Validate();

        BatchConfiguration = batchConfiguration;
        BatchProcessingRequired = true;
    }

    public string Topic { get; }

    public IMessageDeserializer Deserializer { get; }
    
    public BatchConfiguration? BatchConfiguration { get; }

    public bool BatchProcessingRequired { get; }

    public HandlerConfiguration HandlerConfiguration { get; } = new();
}