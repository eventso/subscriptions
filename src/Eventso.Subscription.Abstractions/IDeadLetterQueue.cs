using System.Collections.Generic;

namespace Eventso.Subscription
{
    public interface IDeadLetterQueue
    {
        void Enqueue(DeadLetter message);

        void EnqueueRange(IEnumerable<DeadLetter> messages);
    }
}