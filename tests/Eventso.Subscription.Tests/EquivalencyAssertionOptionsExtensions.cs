using System;
using FluentAssertions;
using FluentAssertions.Equivalency;

namespace Eventso.Subscription.Tests
{
    public static class EquivalencyAssertionOptionsExtensions
    {
        public static EquivalencyAssertionOptions<TExpectation> AcceptingCloseDateTimes<TExpectation>(
            this EquivalencyAssertionOptions<TExpectation> options,
            TimeSpan? precision = default)
        {
            return options
                .Using<DateTime>(c =>
                    c.Subject.Should().BeCloseTo(c.Expectation, precision ?? TimeSpan.FromMilliseconds(1)))
                .WhenTypeIs<DateTime>();
        }

        public static EquivalencyAssertionOptions<TExpectation> ComparingByteReadOnlyMemoryAsArrays<TExpectation>(
            this EquivalencyAssertionOptions<TExpectation> options)
        {
            return options
                .Using<ReadOnlyMemory<byte>>(c =>
                    c.Subject.ToArray().Should().BeEquivalentTo(c.Expectation.ToArray()))
                .WhenTypeIs<ReadOnlyMemory<byte>>();
        }
    }
}