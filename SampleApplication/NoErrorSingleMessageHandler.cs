using Eventso.Subscription;

namespace SampleApplication;

public class NoErrorSingleMessageHandler : IMessageHandler<NoErrorSingleMessageHandler.NoErrorSingleMessage>
{
    public const string Topic = "no-error-single";

    public async Task Handle(NoErrorSingleMessage message, CancellationToken token)
    {
        Console.WriteLine($"[{Topic}] Message received: [{message}]");
        await Task.CompletedTask;
    }

    public record NoErrorSingleMessage(Guid Guid, string String) : ISampleMessage
    {
        public override string ToString()
            => $"Guid: {Guid}, String: {String}";
    };
}