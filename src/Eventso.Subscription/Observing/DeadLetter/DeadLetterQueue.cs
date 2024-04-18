namespace Eventso.Subscription.Observing.DeadLetter;

public sealed class DeadLetterQueue : IDeadLetterQueue
{
    private static readonly Dictionary<object, string> EmptyDeadLetters = new(); 
        
    private readonly object _lockObject = new();

    private Dictionary<object, string>? _deadLetters;

    public void Enqueue(Subscription.DeadLetter deadLetter)
    {
        lock (_lockObject)
        {
            if (_deadLetters == null)
            {
                _deadLetters = new Dictionary<object, string>(1, Subscription.DeadLetter.MessageComparer)
                {
                    [deadLetter.Message] = deadLetter.Reason
                };
                return;
            }

            _deadLetters[deadLetter.Message] = _deadLetters.TryGetValue(deadLetter.Message, out var reason)
                ? JoinReasons(reason, deadLetter.Reason)
                : deadLetter.Reason;
        }
    }

    public void Enqueue(IEnumerable<Subscription.DeadLetter> deadLetters)
    {
        lock (_lockObject)
        {
            _deadLetters ??= new Dictionary<object, string>(1, Subscription.DeadLetter.MessageComparer);
            foreach (var deadLetter in deadLetters)
            {
                _deadLetters[deadLetter.Message] = _deadLetters.TryGetValue(deadLetter.Message, out var reason)
                    ? JoinReasons(reason, deadLetter.Reason)
                    : deadLetter.Reason;
            }
        }
    }

    public IReadOnlyDictionary<object, string> GetDeadLetters()
    {
        // this method will be called after all Enqueue methods in not concurrent conditions
        // ReSharper disable once InconsistentlySynchronizedField
        return _deadLetters ?? EmptyDeadLetters;
    }

    private static string JoinReasons(string initial, string @new)
        => $"{initial}{Environment.NewLine}{Environment.NewLine}{@new}";
}