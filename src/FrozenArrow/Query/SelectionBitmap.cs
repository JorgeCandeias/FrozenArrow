using System.Buffers;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace FrozenArrow.Query;

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
/// 
/// Performance: Uses SIMD (AVX2/AVX-512) for bulk operations when available,
/// processing 4-8 ulong blocks (256-512 bits) per instruction.
/// </remarks>
public struct SelectionBitmap : IDisposable
{
    private ulong[]? _buffer;
    private readonly int _length;
    private readonly int _blockCount;
    
    // Vector sizes for SIMD operations
    private static readonly int Vector256ULongCount = Vector256<ulong>.Count; // 4 ulongs = 256 bits
    private static readonly int Vector512ULongCount = Vector512<ulong>.Count; // 8 ulongs = 512 bits

    /// <summary>
    /// Gets the number of bits in the bitmap.
    /// </summary>
    public readonly int Length => _length;

    /// <summary>
    /// Gets the underlying buffer as a span for advanced scenarios.
    /// </summary>
    public readonly Span<ulong> Blocks => _buffer.AsSpan(0, _blockCount);

    /// <summary>
    /// Gets the underlying buffer array for parallel execution scenarios.
    /// </summary>
    /// <remarks>
    /// This provides direct access to the backing array for use in parallel loops where
    /// Span cannot be captured. The array reference is stable for the lifetime of this
    /// bitmap and can be safely shared across threads for read operations.
    /// For write operations, ensure non-overlapping index ranges per thread.
    /// </remarks>
    internal readonly ulong[]? Buffer => _buffer;

    /// <summary>
    /// Gets the number of ulong blocks in the buffer.
    /// </summary>
    internal readonly int BlockCount => _blockCount;

    /// <summary>
    /// Checks if the bit at the specified index is set, using the provided buffer.
    /// This is a static helper for parallel execution scenarios.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool IsSet(ulong[] buffer, int index)
    {
        var blockIndex = index >> 6;
        var bitIndex = index & 63;
        return (buffer[blockIndex] & (1UL << bitIndex)) != 0;
    }

    /// <summary>
    /// Clears the bit at the specified index, using the provided buffer.
    /// This is a static helper for parallel execution scenarios.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void ClearBit(ulong[] buffer, int index)
    {
        var blockIndex = index >> 6;
        var bitIndex = index & 63;
        // Note: This is not atomic, but safe when threads operate on non-overlapping ranges
        buffer[blockIndex] &= ~(1UL << bitIndex);
    }

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
    /// Uses SIMD when available for improved throughput.
    /// </summary>
    public readonly int CountSet()
    {
        if (_blockCount == 0)
            return 0;

        ref var bufferRef = ref _buffer![0];
        int count = 0;
        int i = 0;

        // Unrolled loop with hardware popcount - this is already highly optimized
        // as BitOperations.PopCount uses the POPCNT instruction when available
        if (_blockCount >= 4)
        {
            int vectorEnd = _blockCount - (_blockCount % 4);
            
            for (; i < vectorEnd; i += 4)
            {
                count += BitOperations.PopCount(Unsafe.Add(ref bufferRef, i));
                count += BitOperations.PopCount(Unsafe.Add(ref bufferRef, i + 1));
                count += BitOperations.PopCount(Unsafe.Add(ref bufferRef, i + 2));
                count += BitOperations.PopCount(Unsafe.Add(ref bufferRef, i + 3));
            }
        }

        // Scalar tail
        for (; i < _blockCount; i++)
        {
            count += BitOperations.PopCount(Unsafe.Add(ref bufferRef, i));
        }
        
        return count;
    }

    /// <summary>
    /// Performs a bitwise AND with another bitmap, modifying this bitmap in place.
    /// Uses SIMD when available for 4-8x throughput improvement.
    /// </summary>
    public void And(SelectionBitmap other)
    {
        if (other._length != _length)
            throw new ArgumentException("Bitmaps must have the same length.", nameof(other));

        if (_blockCount == 0)
            return;

        ref var thisRef = ref _buffer![0];
        ref var otherRef = ref other._buffer![0];
        int i = 0;

        // AVX-512 path: process 8 ulongs (512 bits) per instruction
        if (Vector512.IsHardwareAccelerated && _blockCount >= Vector512ULongCount)
        {
            int vectorEnd = _blockCount - (_blockCount % Vector512ULongCount);
            
            for (; i < vectorEnd; i += Vector512ULongCount)
            {
                var a = Vector512.LoadUnsafe(ref Unsafe.Add(ref thisRef, i));
                var b = Vector512.LoadUnsafe(ref Unsafe.Add(ref otherRef, i));
                Vector512.StoreUnsafe(a & b, ref Unsafe.Add(ref thisRef, i));
            }
        }
        // AVX2 path: process 4 ulongs (256 bits) per instruction
        else if (Vector256.IsHardwareAccelerated && _blockCount >= Vector256ULongCount)
        {
            int vectorEnd = _blockCount - (_blockCount % Vector256ULongCount);
            
            for (; i < vectorEnd; i += Vector256ULongCount)
            {
                var a = Vector256.LoadUnsafe(ref Unsafe.Add(ref thisRef, i));
                var b = Vector256.LoadUnsafe(ref Unsafe.Add(ref otherRef, i));
                Vector256.StoreUnsafe(a & b, ref Unsafe.Add(ref thisRef, i));
            }
        }
        // 128-bit SIMD fallback
        else if (Vector128.IsHardwareAccelerated && _blockCount >= 2)
        {
            int vectorEnd = _blockCount - (_blockCount % 2);
            
            for (; i < vectorEnd; i += 2)
            {
                var a = Vector128.LoadUnsafe(ref Unsafe.Add(ref thisRef, i));
                var b = Vector128.LoadUnsafe(ref Unsafe.Add(ref otherRef, i));
                Vector128.StoreUnsafe(a & b, ref Unsafe.Add(ref thisRef, i));
            }
        }

        // Scalar tail
        for (; i < _blockCount; i++)
        {
            Unsafe.Add(ref thisRef, i) &= Unsafe.Add(ref otherRef, i);
        }
    }

    /// <summary>
    /// Performs a bitwise OR with another bitmap, modifying this bitmap in place.
    /// Uses SIMD when available for 4-8x throughput improvement.
    /// </summary>
    public void Or(SelectionBitmap other)
    {
        if (other._length != _length)
            throw new ArgumentException("Bitmaps must have the same length.", nameof(other));

        if (_blockCount == 0)
            return;

        ref var thisRef = ref _buffer![0];
        ref var otherRef = ref other._buffer![0];
        int i = 0;

        // AVX-512 path
        if (Vector512.IsHardwareAccelerated && _blockCount >= Vector512ULongCount)
        {
            int vectorEnd = _blockCount - (_blockCount % Vector512ULongCount);
            
            for (; i < vectorEnd; i += Vector512ULongCount)
            {
                var a = Vector512.LoadUnsafe(ref Unsafe.Add(ref thisRef, i));
                var b = Vector512.LoadUnsafe(ref Unsafe.Add(ref otherRef, i));
                Vector512.StoreUnsafe(a | b, ref Unsafe.Add(ref thisRef, i));
            }
        }
        // AVX2 path
        else if (Vector256.IsHardwareAccelerated && _blockCount >= Vector256ULongCount)
        {
            int vectorEnd = _blockCount - (_blockCount % Vector256ULongCount);
            
            for (; i < vectorEnd; i += Vector256ULongCount)
            {
                var a = Vector256.LoadUnsafe(ref Unsafe.Add(ref thisRef, i));
                var b = Vector256.LoadUnsafe(ref Unsafe.Add(ref otherRef, i));
                Vector256.StoreUnsafe(a | b, ref Unsafe.Add(ref thisRef, i));
            }
        }
        // 128-bit SIMD fallback
        else if (Vector128.IsHardwareAccelerated && _blockCount >= 2)
        {
            int vectorEnd = _blockCount - (_blockCount % 2);
            
            for (; i < vectorEnd; i += 2)
            {
                var a = Vector128.LoadUnsafe(ref Unsafe.Add(ref thisRef, i));
                var b = Vector128.LoadUnsafe(ref Unsafe.Add(ref otherRef, i));
                Vector128.StoreUnsafe(a | b, ref Unsafe.Add(ref thisRef, i));
            }
        }

        // Scalar tail
        for (; i < _blockCount; i++)
        {
            Unsafe.Add(ref thisRef, i) |= Unsafe.Add(ref otherRef, i);
        }
    }

    /// <summary>
    /// Inverts all bits in the bitmap.
    /// Uses SIMD when available for 4-8x throughput improvement.
    /// </summary>
    public void Not()
    {
        if (_blockCount == 0)
            return;

        ref var bufferRef = ref _buffer![0];
        int i = 0;

        // AVX-512 path
        if (Vector512.IsHardwareAccelerated && _blockCount >= Vector512ULongCount)
        {
            var allOnes = Vector512.Create(ulong.MaxValue);
            int vectorEnd = _blockCount - (_blockCount % Vector512ULongCount);
            
            for (; i < vectorEnd; i += Vector512ULongCount)
            {
                var data = Vector512.LoadUnsafe(ref Unsafe.Add(ref bufferRef, i));
                Vector512.StoreUnsafe(data ^ allOnes, ref Unsafe.Add(ref bufferRef, i));
            }
        }
        // AVX2 path
        else if (Vector256.IsHardwareAccelerated && _blockCount >= Vector256ULongCount)
        {
            var allOnes = Vector256.Create(ulong.MaxValue);
            int vectorEnd = _blockCount - (_blockCount % Vector256ULongCount);
            
            for (; i < vectorEnd; i += Vector256ULongCount)
            {
                var data = Vector256.LoadUnsafe(ref Unsafe.Add(ref bufferRef, i));
                Vector256.StoreUnsafe(data ^ allOnes, ref Unsafe.Add(ref bufferRef, i));
            }
        }
        // 128-bit SIMD fallback
        else if (Vector128.IsHardwareAccelerated && _blockCount >= 2)
        {
            var allOnes = Vector128.Create(ulong.MaxValue);
            int vectorEnd = _blockCount - (_blockCount % 2);
            
            for (; i < vectorEnd; i += 2)
            {
                var data = Vector128.LoadUnsafe(ref Unsafe.Add(ref bufferRef, i));
                Vector128.StoreUnsafe(data ^ allOnes, ref Unsafe.Add(ref bufferRef, i));
            }
        }

        // Scalar tail
        for (; i < _blockCount; i++)
        {
            Unsafe.Add(ref bufferRef, i) = ~Unsafe.Add(ref bufferRef, i);
        }

        // Mask off unused bits in the last block
        var bitsInLastBlock = _length & 63;
        if (bitsInLastBlock != 0)
        {
            _buffer![_blockCount - 1] &= (1UL << bitsInLastBlock) - 1;
        }
    }

    /// <summary>
    /// Performs a bitwise AND-NOT (this &amp; ~other), modifying this bitmap in place.
    /// Useful for excluding items: keeps bits set in this but not in other.
    /// Uses SIMD when available.
    /// </summary>
    public void AndNot(SelectionBitmap other)
    {
        if (other._length != _length)
            throw new ArgumentException("Bitmaps must have the same length.", nameof(other));

        if (_blockCount == 0)
            return;

        ref var thisRef = ref _buffer![0];
        ref var otherRef = ref other._buffer![0];
        int i = 0;

        // AVX-512 path
        if (Vector512.IsHardwareAccelerated && _blockCount >= Vector512ULongCount)
        {
            int vectorEnd = _blockCount - (_blockCount % Vector512ULongCount);
            
            for (; i < vectorEnd; i += Vector512ULongCount)
            {
                var a = Vector512.LoadUnsafe(ref Unsafe.Add(ref thisRef, i));
                var b = Vector512.LoadUnsafe(ref Unsafe.Add(ref otherRef, i));
                Vector512.StoreUnsafe(Vector512.AndNot(b, a), ref Unsafe.Add(ref thisRef, i));
            }
        }
        // AVX2 path
        else if (Vector256.IsHardwareAccelerated && _blockCount >= Vector256ULongCount)
        {
            int vectorEnd = _blockCount - (_blockCount % Vector256ULongCount);
            
            for (; i < vectorEnd; i += Vector256ULongCount)
            {
                var a = Vector256.LoadUnsafe(ref Unsafe.Add(ref thisRef, i));
                var b = Vector256.LoadUnsafe(ref Unsafe.Add(ref otherRef, i));
                Vector256.StoreUnsafe(Vector256.AndNot(b, a), ref Unsafe.Add(ref thisRef, i));
            }
        }
        // 128-bit SIMD fallback
        else if (Vector128.IsHardwareAccelerated && _blockCount >= 2)
        {
            int vectorEnd = _blockCount - (_blockCount % 2);
            
            for (; i < vectorEnd; i += 2)
            {
                var a = Vector128.LoadUnsafe(ref Unsafe.Add(ref thisRef, i));
                var b = Vector128.LoadUnsafe(ref Unsafe.Add(ref otherRef, i));
                Vector128.StoreUnsafe(Vector128.AndNot(b, a), ref Unsafe.Add(ref thisRef, i));
            }
        }

        // Scalar tail
        for (; i < _blockCount; i++)
        {
            Unsafe.Add(ref thisRef, i) &= ~Unsafe.Add(ref otherRef, i);
        }
    }

    /// <summary>
    /// Checks if any bit is set in the bitmap.
    /// Short-circuits on first set bit found.
    /// </summary>
    public readonly bool Any()
    {
        if (_blockCount == 0)
            return false;

        ref var bufferRef = ref _buffer![0];
        int i = 0;

        // AVX-512 path: check 512 bits at once
        if (Vector512.IsHardwareAccelerated && _blockCount >= Vector512ULongCount)
        {
            int vectorEnd = _blockCount - (_blockCount % Vector512ULongCount);
            
            for (; i < vectorEnd; i += Vector512ULongCount)
            {
                var data = Vector512.LoadUnsafe(ref Unsafe.Add(ref bufferRef, i));
                if (data != Vector512<ulong>.Zero)
                    return true;
            }
        }
        // AVX2 path: check 256 bits at once
        else if (Vector256.IsHardwareAccelerated && _blockCount >= Vector256ULongCount)
        {
            int vectorEnd = _blockCount - (_blockCount % Vector256ULongCount);
            
            for (; i < vectorEnd; i += Vector256ULongCount)
            {
                var data = Vector256.LoadUnsafe(ref Unsafe.Add(ref bufferRef, i));
                if (data != Vector256<ulong>.Zero)
                    return true;
            }
        }

        // Scalar tail
        for (; i < _blockCount; i++)
        {
            if (Unsafe.Add(ref bufferRef, i) != 0)
                return true;
        }
        
        return false;
    }

    /// <summary>
    /// Checks if all bits are set in the bitmap.
    /// </summary>
    public readonly bool All()
    {
        if (_blockCount == 0)
            return true;

        ref var bufferRef = ref _buffer![0];
        
        // Check all blocks except the last (which may have unused bits)
        int fullBlocks = _blockCount - 1;
        int i = 0;

        // AVX-512 path
        if (Vector512.IsHardwareAccelerated && fullBlocks >= Vector512ULongCount)
        {
            var allOnes = Vector512.Create(ulong.MaxValue);
            int vectorEnd = fullBlocks - (fullBlocks % Vector512ULongCount);
            
            for (; i < vectorEnd; i += Vector512ULongCount)
            {
                var data = Vector512.LoadUnsafe(ref Unsafe.Add(ref bufferRef, i));
                if (data != allOnes)
                    return false;
            }
        }
        // AVX2 path
        else if (Vector256.IsHardwareAccelerated && fullBlocks >= Vector256ULongCount)
        {
            var allOnes = Vector256.Create(ulong.MaxValue);
            int vectorEnd = fullBlocks - (fullBlocks % Vector256ULongCount);
            
            for (; i < vectorEnd; i += Vector256ULongCount)
            {
                var data = Vector256.LoadUnsafe(ref Unsafe.Add(ref bufferRef, i));
                if (data != allOnes)
                    return false;
            }
        }

        // Scalar tail for full blocks
        for (; i < fullBlocks; i++)
        {
            if (Unsafe.Add(ref bufferRef, i) != ulong.MaxValue)
                return false;
        }

        // Check last block with proper mask
        var bitsInLastBlock = _length & 63;
        var expectedLastBlock = bitsInLastBlock == 0 ? ulong.MaxValue : (1UL << bitsInLastBlock) - 1;
        return _buffer![_blockCount - 1] == expectedLastBlock;
    }

    /// <summary>
    /// Returns an enumerator that yields the indices of all set bits.
    /// </summary>
    public readonly SelectedIndicesEnumerator GetSelectedIndices() => new(this);

    /// <summary>
    /// Applies a bitmask to 8 consecutive bits starting at the specified index.
    /// The mask is an 8-bit value where each bit corresponds to a row.
    /// A 0 bit in the mask clears the corresponding selection bit.
    /// </summary>
    /// <param name="startIndex">The starting bit index (must be 8-aligned for best performance).</param>
    /// <param name="mask">8-bit mask where 1 = keep selected, 0 = deselect.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void ApplyMask8(int startIndex, byte mask)
    {
        // Calculate which ulong block and bit position within it
        var blockIndex = startIndex >> 6;  // startIndex / 64
        var bitOffset = startIndex & 63;   // startIndex % 64
        
        // Create a mask with bits to keep: invert the comparison mask then AND with existing bits
        // Actually, we want to AND with the mask (keep bits where mask is 1)
        var ulongMask = (ulong)mask << bitOffset;
        
        // We need to clear bits where mask is 0, but only in the range [startIndex, startIndex+8)
        // So we create a "clear mask" for those 8 bits
        var clearMask = (ulong)0xFF << bitOffset;  // Bits we're operating on
        
        // Clear the 8 bits, then set only where mask indicates
        _buffer![blockIndex] = (_buffer[blockIndex] & ~clearMask) | (_buffer[blockIndex] & ulongMask);
    }

    /// <summary>
    /// Applies a bitmask to 8 consecutive bits, ANDing with the existing selection.
    /// This is the common case: clear bits where the predicate result is false.
    /// </summary>
    /// <param name="startIndex">The starting bit index (must be 8-aligned for best performance).</param>
    /// <param name="mask">8-bit mask where 1 = keep current, 0 = force clear.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void AndMask8(int startIndex, byte mask)
    {
        var blockIndex = startIndex >> 6;
        var bitOffset = startIndex & 63;
        
        // Create inverse clear mask: 1s everywhere except where we want to potentially clear
        var preserveMask = ~((ulong)0xFF << bitOffset);  // Bits outside our range are preserved
        var andMask = (ulong)mask << bitOffset;          // The actual mask for our 8 bits
        
        _buffer![blockIndex] = (_buffer[blockIndex] & preserveMask) | (_buffer[blockIndex] & andMask);
    }

    /// <summary>
    /// Applies a 4-bit mask to 4 consecutive bits (used for double comparisons with AVX2).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void AndMask4(int startIndex, byte mask)
    {
        var blockIndex = startIndex >> 6;
        var bitOffset = startIndex & 63;
        
        var preserveMask = ~((ulong)0x0F << bitOffset);
        var andMask = (ulong)(mask & 0x0F) << bitOffset;
        
        _buffer![blockIndex] = (_buffer[blockIndex] & preserveMask) | (_buffer[blockIndex] & andMask);
    }

    /// <summary>
    /// Clears all bits in the specified range [startIndex, endIndex).
    /// Uses bulk block operations for efficiency - O(range/64) instead of O(range).
    /// </summary>
    /// <param name="startIndex">The first bit index to clear (inclusive).</param>
    /// <param name="endIndex">The last bit index (exclusive).</param>
    /// <remarks>
    /// This is optimized for clearing large ranges such as entire chunks when zone maps
    /// indicate no possible matches. For a 16K chunk, this performs ~256 ulong operations
    /// instead of 16K individual bit operations.
    /// </remarks>
    public void ClearRange(int startIndex, int endIndex)
    {
        if (startIndex >= endIndex || _blockCount == 0)
            return;

        // Clamp to valid range
        startIndex = Math.Max(0, startIndex);
        endIndex = Math.Min(_length, endIndex);

        var startBlock = startIndex >> 6;      // First block containing bits to clear
        var endBlock = (endIndex - 1) >> 6;    // Last block containing bits to clear
        var startBit = startIndex & 63;        // Bit offset within start block
        var endBit = (endIndex - 1) & 63;      // Bit offset within end block

        ref var bufferRef = ref _buffer![0];

        if (startBlock == endBlock)
        {
            // All bits are in a single block - create a mask for just those bits
            // Mask has 1s for bits to KEEP (outside the range)
            var bitsToKeep = ~(((1UL << (endBit - startBit + 1)) - 1) << startBit);
            Unsafe.Add(ref bufferRef, startBlock) &= bitsToKeep;
            return;
        }

        // Handle partial first block (if startBit > 0)
        if (startBit > 0)
        {
            // Clear bits [startBit, 63] in startBlock
            // Keep bits [0, startBit - 1]
            var keepMask = (1UL << startBit) - 1;
            Unsafe.Add(ref bufferRef, startBlock) &= keepMask;
            startBlock++;
        }

        // Handle partial last block (if endBit < 63)
        if (endBit < 63)
        {
            // Clear bits [0, endBit] in endBlock
            // Keep bits [endBit + 1, 63]
            var keepMask = ~((1UL << (endBit + 1)) - 1);
            Unsafe.Add(ref bufferRef, endBlock) &= keepMask;
            endBlock--;
        }

        // Clear full blocks in the middle using SIMD when possible
        var fullBlockCount = endBlock - startBlock + 1;
        if (fullBlockCount <= 0)
            return;

        int i = startBlock;

        // AVX-512: Clear 8 blocks (512 bits) per iteration
        if (Vector512.IsHardwareAccelerated && fullBlockCount >= Vector512ULongCount)
        {
            var zero = Vector512<ulong>.Zero;
            var vectorEnd = startBlock + fullBlockCount - (fullBlockCount % Vector512ULongCount);
            
            for (; i < vectorEnd; i += Vector512ULongCount)
            {
                Vector512.StoreUnsafe(zero, ref Unsafe.Add(ref bufferRef, i));
            }
        }
        // AVX2: Clear 4 blocks (256 bits) per iteration
        else if (Vector256.IsHardwareAccelerated && fullBlockCount >= Vector256ULongCount)
        {
            var zero = Vector256<ulong>.Zero;
            var vectorEnd = startBlock + fullBlockCount - (fullBlockCount % Vector256ULongCount);
            
            for (; i < vectorEnd; i += Vector256ULongCount)
            {
                Vector256.StoreUnsafe(zero, ref Unsafe.Add(ref bufferRef, i));
            }
        }
        // SSE: Clear 2 blocks (128 bits) per iteration
        else if (Vector128.IsHardwareAccelerated && fullBlockCount >= 2)
        {
            var zero = Vector128<ulong>.Zero;
            var vectorEnd = startBlock + fullBlockCount - (fullBlockCount % 2);
            
            for (; i < vectorEnd; i += 2)
            {
                Vector128.StoreUnsafe(zero, ref Unsafe.Add(ref bufferRef, i));
            }
        }

        // Scalar tail
        for (; i <= endBlock; i++)
        {
            Unsafe.Add(ref bufferRef, i) = 0;
        }
    }

    /// <summary>
    /// Clears all bits in the specified range using the provided buffer.
    /// This is a static helper for parallel execution scenarios.
    /// </summary>
    /// <param name="buffer">The underlying ulong array buffer.</param>
    /// <param name="bufferLength">The total number of bits the buffer represents.</param>
    /// <param name="startIndex">The first bit index to clear (inclusive).</param>
    /// <param name="endIndex">The last bit index (exclusive).</param>
    /// <remarks>
    /// Thread-safe for non-overlapping ranges. Uses SIMD when available.
    /// </remarks>
    internal static void ClearRangeStatic(ulong[] buffer, int bufferLength, int startIndex, int endIndex)
    {
        var blockCount = (bufferLength + 63) >> 6;
        if (startIndex >= endIndex || blockCount == 0)
            return;

        startIndex = Math.Max(0, startIndex);
        endIndex = Math.Min(bufferLength, endIndex);

        var startBlock = startIndex >> 6;
        var endBlock = (endIndex - 1) >> 6;
        var startBit = startIndex & 63;
        var endBit = (endIndex - 1) & 63;

        ref var bufferRef = ref buffer[0];

        if (startBlock == endBlock)
        {
            var bitsToKeep = ~(((1UL << (endBit - startBit + 1)) - 1) << startBit);
            Unsafe.Add(ref bufferRef, startBlock) &= bitsToKeep;
            return;
        }

        if (startBit > 0)
        {
            var keepMask = (1UL << startBit) - 1;
            Unsafe.Add(ref bufferRef, startBlock) &= keepMask;
            startBlock++;
        }

        if (endBit < 63)
        {
            var keepMask = ~((1UL << (endBit + 1)) - 1);
            Unsafe.Add(ref bufferRef, endBlock) &= keepMask;
            endBlock--;
        }

        var fullBlockCount = endBlock - startBlock + 1;
        if (fullBlockCount <= 0)
            return;

        int i = startBlock;

        if (Vector256.IsHardwareAccelerated && fullBlockCount >= Vector256ULongCount)
        {
            var zero = Vector256<ulong>.Zero;
            var vectorEnd = startBlock + fullBlockCount - (fullBlockCount % Vector256ULongCount);
            
            for (; i < vectorEnd; i += Vector256ULongCount)
            {
                Vector256.StoreUnsafe(zero, ref Unsafe.Add(ref bufferRef, i));
            }
        }

        for (; i <= endBlock; i++)
        {
            Unsafe.Add(ref bufferRef, i) = 0;
        }
    }

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
