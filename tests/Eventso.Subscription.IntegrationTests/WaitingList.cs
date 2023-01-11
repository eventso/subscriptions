namespace Eventso.Subscription.IntegrationTests;

public sealed class WaitingList<T> : List<T>
{
    private readonly List<(int count, TaskCompletionSource tcs)> waiters = new();

    public Task WaitUntil(int count, TimeSpan? timeout = default)
    {
        var tcs = new TaskCompletionSource();

        waiters.Add((count, tcs));

        return Task.WhenAny(tcs.Task, TimeoutDelay());

        async Task TimeoutDelay()
        {
            await Task.Delay(timeout ?? TimeSpan.FromSeconds(180));

            if (!tcs.Task.IsCompleted)
                throw new TimeoutException();
        }
    }

    public new void Add(T item)
    {
        base.Add(item);
        Wakeup();
    }

    public new void AddRange(IEnumerable<T> items)
    {
        base.AddRange(items);
        Wakeup();
    }

    private void Wakeup()
    {
        if (waiters.Count == 0)
            return;

        var index = 0;
        while (index < waiters.Count)
        {
            if (waiters[index].count <= this.Count)
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
}