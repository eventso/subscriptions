using Eventso.Subscription;

namespace SampleApplication;

public class NoErrorBatchMessageHandler
    : IMessageHandler<IReadOnlyCollection<NoErrorBatchMessageHandler.NoErrorBatchMessage>>
{
    public const string Topic = "no-error-batch";

    public Task Handle(IReadOnlyCollection<NoErrorBatchMessage> messages, CancellationToken token)
    {
        Console.WriteLine($"[{Topic}] Message batch received ({messages.Count} messages)");

        foreach (var message in messages)
            Console.WriteLine($"\t[{Topic}] Message received: [{message}]");

        return Task.CompletedTask;
    }

    public record NoErrorBatchMessage(Guid Guid, string String) : ISampleMessage
    {
        public override string ToString()
            => $"Guid: {Guid}, String: {String}";
    };
}