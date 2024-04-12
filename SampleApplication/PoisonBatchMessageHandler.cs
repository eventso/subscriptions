using Eventso.Subscription;

namespace SampleApplication;

public class PoisonBatchMessageHandler
    : IMessageHandler<IReadOnlyCollection<PoisonBatchMessageHandler.PoisonBatchMessage>>
{
    public const string Topic = "poison-batch";

    private readonly IDeadLetterQueue _deadLetterQueue;

    public PoisonBatchMessageHandler(IDeadLetterQueue deadLetterQueue)
        => _deadLetterQueue = deadLetterQueue;

    public Task Handle(IReadOnlyCollection<PoisonBatchMessage> messages, CancellationToken token)
    {
        Console.WriteLine($"[{Topic}] Message batch received ({messages.Count} messages).");

        foreach (var message in messages)
        {
            Console.WriteLine($"\t[{Topic}] Message received: [{message}]. Throwing exception: [{message.Bool}].");

            if (message.Bool)
                _deadLetterQueue.Enqueue(new DeadLetter(message, $"[{Topic}] Bool is TRUE in message [{message}]."));
        }

        return Task.CompletedTask;
    }

    public record PoisonBatchMessage(Guid Guid, bool Bool, string String) : ISampleMessage
    {
        public override string ToString()
            => $"Guid: {Guid}, Bool: {Bool}, String: {String}";
    };
}