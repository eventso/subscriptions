namespace Eventso.Subscription;

public interface IConvertibleCollection<T> : IReadOnlyList<T>
{
    IReadOnlyCollection<TOut> Convert<TOut>(Converter<T, TOut> converter);
    bool OnlyContainsSame<TValue>(Func<T, TValue> valueConverter);

    int FindFirstIndexIn(IKeySet<T> set);

    void CopyTo(PooledList<T> target,int start, int length);
}

public interface IKeySet<T>
{
    bool Contains(in T item);
    bool IsEmpty();
}