namespace Eventso.Subscription;

public interface IGroupedMetadataProvider<in TItem>
{
    List<KeyValuePair<string, object>[]> GetFor(IEnumerable<TItem> items);
}
