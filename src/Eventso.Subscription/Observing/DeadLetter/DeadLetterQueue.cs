using System;
using System.Collections.Generic;

namespace Eventso.Subscription.Observing.DeadLetter
{
    public sealed class DeadLetterQueue : IDeadLetterQueue
    {
        private static readonly Dictionary<object, IReadOnlySet<string>> EmptyDeadLetters = new(); 
        
        private readonly object _lockObject = new();

        private Dictionary<object, IReadOnlySet<string>> _knownDeadLetters;

        public void Enqueue(Subscription.DeadLetter deadLetter)
        {
            lock (_lockObject)
            {
                _knownDeadLetters ??= new Dictionary<object, IReadOnlySet<string>>(1, Subscription.DeadLetter.MessageComparer);
                if (!_knownDeadLetters.TryGetValue(deadLetter.Message, out var reasons))
                    _knownDeadLetters[deadLetter.Message] = reasons = new HashSet<string>(1);

                AddReason(reasons, deadLetter.Reason);
            }
        }

        public void EnqueueRange(IEnumerable<Subscription.DeadLetter> deadLetters)
        {
            lock (_lockObject)
            {
                _knownDeadLetters ??= new Dictionary<object, IReadOnlySet<string>>(1, Subscription.DeadLetter.MessageComparer);
                foreach (var deadLetter in deadLetters)
                {
                    if (!_knownDeadLetters.TryGetValue(deadLetter.Message, out var reasons))
                        _knownDeadLetters[deadLetter.Message] = reasons = new HashSet<string>(1);

                    AddReason(reasons, deadLetter.Reason);
                }
            }
        }

        public IReadOnlyDictionary<object, IReadOnlySet<string>> GetDeadLetters()
        {
            // this method will be called after all Enqueue methods in not concurrent conditions
            // ReSharper disable once InconsistentlySynchronizedField
            return _knownDeadLetters ?? EmptyDeadLetters;
        }

        private static void AddReason(IReadOnlySet<string> reasons, string reason)
        {
            // dirty hack with cast but we are sure that it is a hashset and it gives us clearer public api
            if (reasons is not HashSet<string> hashSet)
                throw new InvalidOperationException(
                    $"Expected {typeof(HashSet<string>).FullName} object with reasons, but got {reasons.GetType().FullName}");

            hashSet.Add(reason);
        }
    }
}