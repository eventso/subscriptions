namespace Eventso.Subscription;

public interface IGroupedMetadataProvider<in TItem>
{
    KeyValuePair<string, object>[] GetFor(IEnumerable<TItem> items);
}
