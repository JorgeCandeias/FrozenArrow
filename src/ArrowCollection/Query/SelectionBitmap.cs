using System.Buffers;
using System.Numerics;
using System.Runtime.CompilerServices;

namespace ArrowCollection.Query;

/// <summary>
/// A pooled selection bitmap using a compact bitfield representation.
/// Uses ArrayPool for the backing storage to minimize allocations.
/// </summary>
/// <remarks>
/// Memory efficiency: 8x more compact than bool[] (1 bit vs 1 byte per element).
/// For 1M items: 125 KB vs 1 MB.
/// 
/// This type implements IDisposable and MUST be disposed to return the buffer to the pool.
/// Use with 'using' statement or declaration.
/// </remarks>
public struct SelectionBitmap : IDisposable
{
    private ulong[]? _buffer;
    private readonly int _length;
    private readonly int _blockCount;

    /// <summary>
    /// Gets the number of bits in the bitmap.
    /// </summary>
    public readonly int Length => _length;

    /// <summary>
    /// Gets the underlying buffer as a span for advanced scenarios.
    /// </summary>
    public readonly Span<ulong> Blocks => _buffer.AsSpan(0, _blockCount);

    /// <summary>
    /// Creates a new SelectionBitmap with all bits set to the specified initial value.
    /// </summary>
    /// <param name="length">The number of bits in the bitmap.</param>
    /// <param name="initialValue">The initial value for all bits (default: true/selected).</param>
    /// <returns>A new SelectionBitmap. Must be disposed when no longer needed.</returns>
    public static SelectionBitmap Create(int length, bool initialValue = true)
    {
        if (length < 0)
            throw new ArgumentOutOfRangeException(nameof(length), "Length must be non-negative.");

        if (length == 0)
            return new SelectionBitmap([], 0, 0);

        var blockCount = (length + 63) >> 6; // Divide by 64, round up
        var buffer = ArrayPool<ulong>.Shared.Rent(blockCount);

        if (initialValue)
        {
            // Set all bits to 1
            Array.Fill(buffer, ulong.MaxValue, 0, blockCount);

            // Mask off unused bits in the last block
            var bitsInLastBlock = length & 63; // length % 64
            if (bitsInLastBlock != 0)
            {
                buffer[blockCount - 1] = (1UL << bitsInLastBlock) - 1;
            }
        }
        else
        {
            // Set all bits to 0
            Array.Fill(buffer, 0UL, 0, blockCount);
        }

        return new SelectionBitmap(buffer, length, blockCount);
    }

    private SelectionBitmap(ulong[] buffer, int length, int blockCount)
    {
        _buffer = buffer;
        _length = length;
        _blockCount = blockCount;
    }

    /// <summary>
    /// Gets or sets the bit at the specified index.
    /// </summary>
    public bool this[int index]
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        readonly get
        {
            var blockIndex = index >> 6; // index / 64
            var bitIndex = index & 63;   // index % 64
            return (_buffer![blockIndex] & (1UL << bitIndex)) != 0;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        set
        {
            var blockIndex = index >> 6;
            var bitIndex = index & 63;
            if (value)
                _buffer![blockIndex] |= (1UL << bitIndex);
            else
                _buffer![blockIndex] &= ~(1UL << bitIndex);
        }
    }

    /// <summary>
    /// Clears the bit at the specified index (sets to false/unselected).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Clear(int index)
    {
        var blockIndex = index >> 6;
        var bitIndex = index & 63;
        _buffer![blockIndex] &= ~(1UL << bitIndex);
    }

    /// <summary>
    /// Sets the bit at the specified index (sets to true/selected).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Set(int index)
    {
        var blockIndex = index >> 6;
        var bitIndex = index & 63;
        _buffer![blockIndex] |= (1UL << bitIndex);
    }

    /// <summary>
    /// Counts the number of set bits (selected items) using hardware popcount.
    /// </summary>
    public readonly int CountSet()
    {
        int count = 0;
        for (int i = 0; i < _blockCount; i++)
        {
            count += BitOperations.PopCount(_buffer![i]);
        }
        return count;
    }

    /// <summary>
    /// Performs a bitwise AND with another bitmap, modifying this bitmap in place.
    /// Used to combine multiple predicates.
    /// </summary>
    public void And(SelectionBitmap other)
    {
        if (other._length != _length)
            throw new ArgumentException("Bitmaps must have the same length.", nameof(other));

        for (int i = 0; i < _blockCount; i++)
        {
            _buffer![i] &= other._buffer![i];
        }
    }

    /// <summary>
    /// Performs a bitwise OR with another bitmap, modifying this bitmap in place.
    /// </summary>
    public void Or(SelectionBitmap other)
    {
        if (other._length != _length)
            throw new ArgumentException("Bitmaps must have the same length.", nameof(other));

        for (int i = 0; i < _blockCount; i++)
        {
            _buffer![i] |= other._buffer![i];
        }
    }

    /// <summary>
    /// Inverts all bits in the bitmap.
    /// </summary>
    public void Not()
    {
        for (int i = 0; i < _blockCount; i++)
        {
            _buffer![i] = ~_buffer[i];
        }

        // Mask off unused bits in the last block
        var bitsInLastBlock = _length & 63;
        if (bitsInLastBlock != 0)
        {
            _buffer![_blockCount - 1] &= (1UL << bitsInLastBlock) - 1;
        }
    }

    /// <summary>
    /// Returns an enumerator that yields the indices of all set bits.
    /// </summary>
    public readonly SelectedIndicesEnumerator GetSelectedIndices() => new(this);

    /// <summary>
    /// Disposes the bitmap and returns the buffer to the pool.
    /// </summary>
    public void Dispose()
    {
        if (_buffer is not null && _buffer.Length > 0)
        {
            ArrayPool<ulong>.Shared.Return(_buffer);
            _buffer = null;
        }
    }

    /// <summary>
    /// Enumerator for iterating over selected (set) bit indices.
    /// </summary>
    public ref struct SelectedIndicesEnumerator
    {
        private readonly SelectionBitmap _bitmap;
        private int _blockIndex;
        private ulong _currentBlock;
        private int _current;

        internal SelectedIndicesEnumerator(SelectionBitmap bitmap)
        {
            _bitmap = bitmap;
            _blockIndex = 0;
            _currentBlock = bitmap._blockCount > 0 ? bitmap._buffer![0] : 0;
            _current = -1;
        }

        public readonly int Current => _current;

        public bool MoveNext()
        {
            while (_blockIndex < _bitmap._blockCount)
            {
                if (_currentBlock != 0)
                {
                    // Find the lowest set bit
                    var bitIndex = BitOperations.TrailingZeroCount(_currentBlock);
                    _current = (_blockIndex << 6) + bitIndex;

                    // Check bounds (last block may have unused bits)
                    if (_current >= _bitmap._length)
                        return false;

                    // Clear the bit we just found
                    _currentBlock &= _currentBlock - 1;
                    return true;
                }

                // Move to next block
                _blockIndex++;
                if (_blockIndex < _bitmap._blockCount)
                {
                    _currentBlock = _bitmap._buffer![_blockIndex];
                }
            }

            return false;
        }

        public readonly SelectedIndicesEnumerator GetEnumerator() => this;
    }
}
