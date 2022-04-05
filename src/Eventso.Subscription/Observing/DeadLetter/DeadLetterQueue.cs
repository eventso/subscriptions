using System;
using System.Collections.Generic;
using System.Linq;

namespace Eventso.Subscription.Observing.DeadLetter
{
    public sealed class DeadLetterQueue : IDeadLetterQueue
    {
        private readonly object _lockObject = new();

        private Dictionary<object, HashSet<string>> _knownDeadLetters;

        public void Enqueue(Subscription.DeadLetter deadLetter)
        {
            lock (_lockObject)
            {
                _knownDeadLetters ??= new Dictionary<object, HashSet<string>>(1, Subscription.DeadLetter.MessageComparer);
                if (!_knownDeadLetters.TryGetValue(deadLetter.Message, out var reasons))
                    _knownDeadLetters[deadLetter.Message] = reasons = new HashSet<string>(1);

                reasons.Add(deadLetter.Reason);
            }
        }

        public void EnqueueRange(IEnumerable<Subscription.DeadLetter> deadLetters)
        {
            lock (_lockObject)
            {
                _knownDeadLetters ??= new Dictionary<object, HashSet<string>>(1, Subscription.DeadLetter.MessageComparer);
                foreach (var deadLetter in deadLetters)
                {
                    if (!_knownDeadLetters.TryGetValue(deadLetter.Message, out var reasons))
                        _knownDeadLetters[deadLetter.Message] = reasons = new HashSet<string>(1);

                    reasons.Add(deadLetter.Reason);
                }

            }
        }

        public IReadOnlyCollection<Subscription.DeadLetter> GetDeadLetters()
        {
            // this method will be called after all Enqueue methods in not concurrent conditions
            // ReSharper disable once InconsistentlySynchronizedField
            return _knownDeadLetters?
                .Select(p => new Subscription.DeadLetter(p.Key, string.Join("<-- REASON END -->", p.Value)))
                .ToArray() ?? Array.Empty<Subscription.DeadLetter>();
        }
    }
}