namespace Eventso.Subscription;

public interface IGroupedMetadata<in TSelf>
    where TSelf : IGroupedMetadata<TSelf>
{
    static abstract IEnumerable<KeyValuePair<string, object>[]> GroupedMetadata(IEnumerable<TSelf> items);
}
