namespace Eventso.Subscription;

public interface IGroupedMetadataProvider<in TItem>
{
    List<Dictionary<string, object>> GetFor(IEnumerable<TItem> items);
}
