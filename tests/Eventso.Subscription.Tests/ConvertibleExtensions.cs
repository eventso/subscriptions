namespace Eventso.Subscription.Tests;

public static class ConvertibleExtensions
{
    public static IConvertibleCollection<T> ToConvertibleCollection<T>(
        this IEnumerable<T> items)
    {
        var list = new PooledList<T>(4);
        foreach (var item in items)
            list.Add(item);

        return list;
    }
}