using Confluent.Kafka;
using Eventso.Subscription.Kafka;

namespace Eventso.Subscription.Tests;

public class PrettyOffsetRangeTests
{

    [MemberData(nameof(TestCases))]
    [Theory]
    public void ProducesProperSortedRangeString(IEnumerable<Offset> offsets, string expected)
    {
        var range = new PrettyOffsetRange(offsets);
        range.Compact();

        range.ToString().Should().Be(expected);
    }

    public static IEnumerable<object[]> TestCases()
    {
        yield return new object[]
        {
            // sorted contiguous
            Enumerable.Range(1, 1000)
                .Select(i => new Offset(i)),
            "1-1000"
        };

        yield return new object[]
        {
            // sorted gaps
            Enumerable.Range(1, 1000).Where(i => i % 250 != 0)
                .Select(i => new Offset(i)),
            "1-249,251-499,501-749,751-999"
        };

        yield return new object[]
        {
            // shuffled contiguous
            Enumerable.Range(1, 1000)
                .OrderBy(_ => Guid.NewGuid())
                .Select(i => new Offset(i)),
            "1-1000"
        };

        yield return new object[]
        {
            // shuffled gaps
            Enumerable.Range(1, 1000).Where(i => i % 250 != 0)
                .OrderBy(_ => Guid.NewGuid())
                .Select(i => new Offset(i)),
            "1-249,251-499,501-749,751-999"
        };
    }
}
