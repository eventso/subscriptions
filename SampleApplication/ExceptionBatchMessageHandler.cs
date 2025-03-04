using Eventso.Subscription;

namespace SampleApplication;

public class ExceptionBatchMessageHandler
    : IMessageHandler<IReadOnlyCollection<ExceptionBatchMessageHandler.ExceptionBatchMessage>>
{
    public const string Topic = "exception-batch";

    public Task Handle(IReadOnlyCollection<ExceptionBatchMessage> messages, CancellationToken token)
    {
        Console.WriteLine($"[{Topic}] Message batch received ({messages.Count} messages).");

        foreach (var message in messages)
        {
            Console.WriteLine($"\t[{Topic}] Message received: [{message}]. Throwing exception: [{message.Bool}].");

            if (message.Bool)
                throw new Exception($"[{Topic}] Bool is TRUE in message [{message}].");
        }

        return Task.CompletedTask;
    }

    public record ExceptionBatchMessage(Guid Guid, bool Bool, string String) : ISampleMessage
    {
        public override string ToString()
            => $"Guid: {Guid}, Bool: {Bool}, String: {String}";
    };
}