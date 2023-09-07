using System.Runtime.InteropServices;
using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.ObjectPool;

namespace Eventso.Subscription.Kafka;

internal readonly struct PrettyOffsetRange
{
    private static readonly ObjectPool<StringBuilder> StringBuilderPool =
        new DefaultObjectPool<StringBuilder>(new StringBuilderPooledObjectPolicy());

    private readonly List<Range> _result;

    public PrettyOffsetRange()
    {
        _result = new();
    }

    public PrettyOffsetRange(IEnumerable<Offset> offsets)
    {
        this = new PrettyOffsetRange();
        foreach (var offset in offsets)
        {
            Add(offset);
        }
    }

    /// <summary>
    /// Optimized for sorted addition.
    /// If insert is in order, <see cref="Compact"/> is a noop.
    /// </summary>
    public void Add(Offset offset)
    {
        long value = offset;

        var span = CollectionsMarshal.AsSpan(_result);
        foreach (ref var range in span)
        {
            if (value - range.End == 1L)
            {
                range = new(range.Start, value);
                return;
            }
        }

        _result.Add(new(value));
    }

    public void Compact()
    {
        // single contiguous range
        if (_result.Count < 2) return;

        _result.Sort();

        // merge
        var count = _result.Count;
        var prev = _result[count - 1];
        for (int idx = count - 2; idx >= 0; idx--)
        {
            var next = _result[idx];
            if (next.End == prev.Start - 1)
            {
                prev = _result[idx] = new(next.Start, prev.End);
                _result.RemoveAt(idx + 1);
            }
            else
            {
                prev = next;
            }
        }
    }

    public override string ToString()
    {
        if (_result is null || _result.Count == 0) return "Empty";

        if (_result.Count == 1)
        {
            var single = _result[0];
            return single.ToString();
        }

        var sb = StringBuilderPool.Get();
        foreach (var range in _result)
        {
            if (sb.Length > 0) sb.Append(',');

            // rely on StringBuilder.AppendInterpolatedStringHandler.AppendFormatted + ISpanFormattable
            sb.Append($"{range}");
        }

        var result = sb.ToString();
        StringBuilderPool.Return(sb);

        return result;
    }

    private readonly struct Range : IComparable<Range>, ISpanFormattable
    {
        public readonly long Start;
        public readonly long End;

        public Range(long start, long end)
        {
            Start = start;
            End = end;
        }

        public Range(long single)
        {
            Start = single;
            End = single;
        }

        public bool IsDegenerate => Start == End;

        public int CompareTo(Range other)
        {
            int res = Start.CompareTo(other.Start);
            return res != 0 ? res : End.CompareTo(other.End);
        }

        public override string ToString() => this.ToString(default, default);

        public string ToString(string? format, IFormatProvider? formatProvider)
        {
            // rely on DefaultInterpolatedStringHandler.AppendFormatted<T> + ISpanFormattable
            return string.Create(formatProvider, $"{this}");
        }

        public bool TryFormat(Span<char> destination, out int charsWritten, ReadOnlySpan<char> format, IFormatProvider? provider)
        {
            if (!Start.TryFormat(destination, out charsWritten, format, provider))
                return false;

            if (IsDegenerate)
                return true;

            if (destination.Length <= charsWritten)
                return false;

            destination[charsWritten++] = '-';
            destination = destination.Slice(charsWritten);

            bool res = End.TryFormat(destination,
                out var endWritten,
                format,
                provider);
            charsWritten += endWritten;
            return res;
        }
    }
}
