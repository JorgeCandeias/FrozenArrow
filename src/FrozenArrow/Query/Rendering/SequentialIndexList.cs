namespace FrozenArrow.Query.Rendering;

/// <summary>
/// Represents a sequential range of indices [start..end] without allocating storage.
/// Used for full-scan queries to avoid O(n) memory allocation.
/// </summary>
internal sealed class SequentialIndexList : IReadOnlyList<int>
{
    private readonly int _start;
    private readonly int _count;

    public SequentialIndexList(int start, int count)
    {
        if (start < 0)
            throw new ArgumentOutOfRangeException(nameof(start), "Start must be non-negative.");
        if (count < 0)
            throw new ArgumentOutOfRangeException(nameof(count), "Count must be non-negative.");

        _start = start;
        _count = count;
    }

    public int Count => _count;

    public int this[int index]
    {
        get
        {
            if ((uint)index >= (uint)_count)
                throw new ArgumentOutOfRangeException(nameof(index));
            return _start + index;
        }
    }

    public IEnumerator<int> GetEnumerator()
    {
        for (int i = 0; i < _count; i++)
            yield return _start + i;
    }

    System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
}
