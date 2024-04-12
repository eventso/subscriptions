using Eventso.Subscription;

namespace SampleApplication;

public class PoisonSingleMessageHandler : IMessageHandler<PoisonSingleMessageHandler.PoisonSingleMessage>
{
    public const string Topic = "poison-single";

    private readonly IDeadLetterQueue _deadLetterQueue;

    public PoisonSingleMessageHandler(IDeadLetterQueue deadLetterQueue)
        => _deadLetterQueue = deadLetterQueue;

    public Task Handle(PoisonSingleMessage message, CancellationToken token)
    {
        Console.WriteLine($"[{Topic}] Message received: [{message}]. Is poison: [{message.Bool}].");

        if (message.Bool)
            _deadLetterQueue.Enqueue(new DeadLetter(message, $"[{Topic}] Bool is TRUE in message [{message}]."));

        return Task.CompletedTask;
    }

    public record PoisonSingleMessage(Guid Guid, bool Bool, string String) : ISampleMessage
    {
        public override string ToString()
            => $"Guid: {Guid}, Bool: {Bool}, String: {String}";
    };
}