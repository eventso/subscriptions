using System.Collections;

namespace Eventso.Subscription.IntegrationTests;

public sealed class WaitingCollection<T> : ICollection<T>
    where T : IEquatable<T>
{
    private readonly ICollection<T> _inner;
    private readonly List<(int count, TaskCompletionSource tcs)> waiters = new();
    private readonly List<Fail> _fails = new();

    public WaitingCollection(ICollection<T> inner)
    {
        _inner = inner;
    }

    public void FailOn(T message, int count = 1, Exception e = null)
    {
        _fails.Add(new(message, count, e));
    }

    public void FailOn(IEnumerable<T> messages, int count = 1, Exception e = null)
    {
        foreach (var message in messages)
            FailOn(message, count, e);
    }

    public Task WaitUntil(int count, TimeSpan? timeout = default)
    {
        var tcs = new TaskCompletionSource();

        waiters.Add((count, tcs));

        Wakeup();
        return Task.WhenAny(tcs.Task, TimeoutDelay());

        async Task TimeoutDelay()
        {
            await Task.Delay(timeout ?? TimeSpan.FromSeconds(180));

            if (!tcs.Task.IsCompleted)
                throw new TimeoutException();
        }
    }

    public void Add(T item)
    {
        foreach (var fail in _fails)
            fail.OnMessageReceived(item);

        _inner.Add(item);
        Wakeup();
    }

    public void Clear()
        => _inner.Clear();

    public bool Contains(T item)
        => _inner.Contains(item);

    public void CopyTo(T[] array, int arrayIndex)
        => _inner.CopyTo(array, arrayIndex);

    public bool Remove(T item)
        => _inner.Remove(item);

    public int Count => _inner.Count;

    public bool IsReadOnly => _inner.IsReadOnly;

    public void AddRange(IEnumerable<T> items)
    {
        foreach (var item in items)
            Add(item);
    }

    private void Wakeup()
    {
        if (waiters.Count == 0)
            return;

        var index = 0;
        while (index < waiters.Count)
        {
            if (waiters[index].count <= _inner.Count)
            {
                waiters[index].tcs.SetResult();
                waiters.RemoveAt(index);
            }
            else
            {
                index++;
            }
        }
    }

    private class Fail
    {
        private readonly T _failMessage;
        private int _count;
        private readonly Exception _ex;

        public Fail(T failMessage, int count, Exception ex)
        {
            _failMessage = failMessage;
            _count = count;
            _ex = ex;
        }

        public void OnMessageReceived(T message)
        {
            if (_count > 0 && (_failMessage == null || _failMessage.Equals(message)))
            {
                _count--;
                throw (_ex ?? new FailException(message, _count + 1));
            }
        }
    }

    public class FailException : Exception
    {
        public T SourceMessage { get; }
        public int Count { get; }

        public FailException(T message, int count)
        {
            SourceMessage = message;
            Count = count;
        }
    }

    public IEnumerator<T> GetEnumerator()
        => _inner.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator()
        => _inner.GetEnumerator();
}