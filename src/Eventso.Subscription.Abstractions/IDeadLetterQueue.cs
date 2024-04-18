namespace Eventso.Subscription;

public interface IDeadLetterQueue
{
    void Enqueue(DeadLetter message);

    void Enqueue(IEnumerable<DeadLetter> messages);
}