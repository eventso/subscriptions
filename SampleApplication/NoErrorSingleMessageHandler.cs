using Eventso.Subscription;

namespace SampleApplication;

public class NoErrorSingleMessageHandler : IMessageHandler<NoErrorSingleMessageHandler.NoErrorSingleMessage>
{
    public const string Topic = "no-error-single";

    public Task Handle(NoErrorSingleMessage message, CancellationToken token)
    {
        Console.WriteLine($"[{Topic}] Message received: [{message}]");
        return Task.CompletedTask;
    }

    public record NoErrorSingleMessage(Guid Guid, string String) : ISampleMessage
    {
        public override string ToString()
            => $"Guid: {Guid}, String: {String}";
    };
}