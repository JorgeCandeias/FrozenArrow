using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using Apache.Arrow;

namespace FrozenArrow.Query;

/// <summary>
/// Base class for predicates that can be evaluated directly against Arrow columns
/// without materializing objects.
/// </summary>
public abstract class ColumnPredicate
{
    /// <summary>
    /// Gets the name of the column this predicate operates on.
    /// </summary>
    public abstract string ColumnName { get; }

    /// <summary>
    /// Gets the column index in the record batch.
    /// This is immutable and set at construction time for thread-safety.
    /// </summary>
    public abstract int ColumnIndex { get; }

    /// <summary>
    /// Evaluates this predicate against the column, updating the selection bitmap.
    /// Only rows that are already selected (true) will be evaluated.
    /// </summary>
    /// <param name="batch">The Arrow record batch.</param>
    /// <param name="selection">The selection bitmap to update. True = selected, False = filtered out.</param>
    public abstract void Evaluate(RecordBatch batch, Span<bool> selection);

    /// <summary>
    /// Evaluates this predicate against the column, updating the selection bitmap.
    /// This overload is used when the selection is stored in an array.
    /// </summary>
    public void Evaluate(RecordBatch batch, bool[] selection)
    {
        Evaluate(batch, selection.AsSpan());
    }

    /// <summary>
    /// Evaluates this predicate against the column, updating the compact selection bitmap.
    /// </summary>
    /// <param name="batch">The Arrow record batch.</param>
    /// <param name="selection">The compact selection bitmap to update.</param>
    /// <param name="endIndex">Optional end index (exclusive). If null, evaluates all rows.</param>
    public virtual void Evaluate(RecordBatch batch, ref SelectionBitmap selection, int? endIndex = null)
    {
        var column = batch.Column(ColumnIndex);
        var actualEndIndex = endIndex ?? batch.Length;
        EvaluateRange(column, ref selection, 0, actualEndIndex);
    }

    /// <summary>
    /// Evaluates this predicate for a range of rows. This method is used for parallel execution.
    /// </summary>
    /// <param name="column">The column to evaluate against.</param>
    /// <param name="selection">The selection bitmap to update.</param>
    /// <param name="startIndex">The first row index (inclusive).</param>
    /// <param name="endIndex">The last row index (exclusive).</param>
    /// <remarks>
    /// This method is thread-safe for non-overlapping ranges, allowing multiple threads
    /// to evaluate different row ranges concurrently on the same bitmap.
    /// </remarks>
    public virtual void EvaluateRange(IArrowArray column, ref SelectionBitmap selection, int startIndex, int endIndex)
    {
        // Default implementation: iterate and update bitmap
        // Subclasses can override for better performance (e.g., SIMD)
        for (int i = startIndex; i < endIndex; i++)
        {
            if (!selection[i]) continue; // Already filtered out
            
            // Evaluate using single-item logic
            if (!EvaluateSingle(column, i))
            {
                selection.Clear(i);
            }
        }
    }

    /// <summary>
    /// Evaluates this predicate for a range of rows using the raw selection buffer.
    /// This overload is used for parallel execution where we cannot capture ref structs.
    /// </summary>
    /// <param name="column">The column to evaluate against.</param>
    /// <param name="selectionBuffer">The raw ulong array backing the selection bitmap.</param>
    /// <param name="startIndex">The first row index (inclusive).</param>
    /// <param name="endIndex">The last row index (exclusive).</param>
    /// <remarks>
    /// Thread-safe for non-overlapping ranges. Uses the static helpers in SelectionBitmap
    /// for bit manipulation without needing a ref to the struct.
    /// </remarks>
    public virtual void EvaluateRangeWithBuffer(IArrowArray column, ulong[] selectionBuffer, int startIndex, int endIndex)
    {
        // Default implementation: iterate and update bitmap using static helpers
        for (int i = startIndex; i < endIndex; i++)
        {
            if (!SelectionBitmap.IsSet(selectionBuffer, i)) continue; // Already filtered out
            
            if (!EvaluateSingle(column, i))
            {
                SelectionBitmap.ClearBit(selectionBuffer, i);
            }
        }
    }

    /// <summary>
    /// Evaluates this predicate for a single row.
    /// Override in subclasses for optimized bitmap evaluation.
    /// </summary>
    protected virtual bool EvaluateSingle(IArrowArray column, int index)
    {
        // Default: not implemented, subclasses should override EvaluateRange instead
        throw new NotImplementedException("Subclass must override either EvaluateSingle or EvaluateRange");
    }

    /// <summary>
    /// Evaluates this predicate for a single row. Public version for fused execution.
    /// </summary>
    /// <param name="column">The column to evaluate against.</param>
    /// <param name="index">The row index to evaluate.</param>
    /// <returns>True if the row passes the predicate, false otherwise.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public virtual bool EvaluateSingleRow(IArrowArray column, int index)
    {
        return EvaluateSingle(column, index);
    }

    /// <summary>
    /// Tests if a chunk can potentially contain matching rows based on zone map min/max values.
    /// Returns true if the chunk might contain matches (must evaluate), false if it definitely doesn't (can skip).
    /// </summary>
    /// <param name="zoneMapData">The zone map data for this column.</param>
    /// <param name="chunkIndex">The chunk index to test.</param>
    /// <returns>True if the chunk should be evaluated, false if it can be skipped.</returns>
    public virtual bool MayContainMatches(ColumnZoneMapData? zoneMapData, int chunkIndex)
    {
        // Default: conservative - assume chunk may contain matches
        // Subclasses override for specific predicate types
        return true;
    }
}

/// <summary>
/// Comparison operators for numeric predicates.
/// </summary>
public enum ComparisonOperator
{
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual
}

/// <summary>
/// Base SIMD comparison operations. Every ComparisonOperator can be expressed
/// as one of these three base operations, optionally bitwise-negated.
/// </summary>
internal enum SimdBaseOp : byte
{
    Equals = 0,
    LessThan = 1,
    GreaterThan = 2
}

/// <summary>
/// Pre-computed decomposition of a ComparisonOperator into a base SIMD operation
/// and a negate flag. Resolved once before a SIMD loop to eliminate per-iteration switch overhead.
/// </summary>
internal readonly struct ComparisonDecomposition
{
    public readonly SimdBaseOp BaseOp;
    public readonly bool Negate;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ComparisonDecomposition(ComparisonOperator op)
    {
        // Decompose: Equal=Equals, NotEqual=~Equals, LessThan=LessThan,
        // LessThanOrEqual=~GreaterThan, GreaterThan=GreaterThan, GreaterThanOrEqual=~LessThan
        (BaseOp, Negate) = op switch
        {
            ComparisonOperator.Equal => (SimdBaseOp.Equals, false),
            ComparisonOperator.NotEqual => (SimdBaseOp.Equals, true),
            ComparisonOperator.LessThan => (SimdBaseOp.LessThan, false),
            ComparisonOperator.LessThanOrEqual => (SimdBaseOp.GreaterThan, true),
            ComparisonOperator.GreaterThan => (SimdBaseOp.GreaterThan, false),
            ComparisonOperator.GreaterThanOrEqual => (SimdBaseOp.LessThan, true),
            _ => (SimdBaseOp.Equals, false)
        };
    }

    /// <summary>
    /// Performs the pre-resolved SIMD comparison on Int32 vectors.
    /// The 3-way branch on BaseOp is perfectly predicted since the value is loop-invariant.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Vector256<int> Compare(Vector256<int> data, Vector256<int> compareValue)
    {
        var result = BaseOp switch
        {
            SimdBaseOp.Equals => Vector256.Equals(data, compareValue),
            SimdBaseOp.LessThan => Vector256.LessThan(data, compareValue),
            SimdBaseOp.GreaterThan => Vector256.GreaterThan(data, compareValue),
            _ => Vector256<int>.Zero
        };
        return Negate ? ~result : result;
    }

    /// <summary>
    /// Performs the pre-resolved SIMD comparison on Double vectors.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Vector256<double> Compare(Vector256<double> data, Vector256<double> compareValue)
    {
        var result = BaseOp switch
        {
            SimdBaseOp.Equals => Vector256.Equals(data, compareValue),
            SimdBaseOp.LessThan => Vector256.LessThan(data, compareValue),
            SimdBaseOp.GreaterThan => Vector256.GreaterThan(data, compareValue),
            _ => Vector256<double>.Zero
        };
        return Negate ? ~result : result;
    }
}

/// <summary>
/// Predicate for comparing an Int32 column against a constant value.
/// Uses SIMD vectorization for bulk comparison when processing primitive Int32Arrays.
/// </summary>
public sealed class Int32ComparisonPredicate : ColumnPredicate
{
    public override string ColumnName { get; }
    public override int ColumnIndex { get; }
    public ComparisonOperator Operator { get; }
    public int Value { get; }

    public Int32ComparisonPredicate(string columnName, int columnIndex, ComparisonOperator op, int value)
    {
        ColumnName = columnName;
        ColumnIndex = columnIndex;
        Operator = op;
        Value = value;
    }

    public override void Evaluate(RecordBatch batch, Span<bool> selection)
    {
        var column = batch.Column(ColumnIndex);
        var length = batch.Length;

        for (int i = 0; i < length; i++)
        {
            if (!selection[i]) continue; // Already filtered out

            if (column.IsNull(i))
            {
                selection[i] = false;
                continue;
            }

            var columnValue = RunLengthEncodedArrayBuilder.GetInt32Value(column, i);
            selection[i] = Operator switch
            {
                ComparisonOperator.Equal => columnValue == Value,
                ComparisonOperator.NotEqual => columnValue != Value,
                ComparisonOperator.LessThan => columnValue < Value,
                ComparisonOperator.LessThanOrEqual => columnValue <= Value,
                ComparisonOperator.GreaterThan => columnValue > Value,
                ComparisonOperator.GreaterThanOrEqual => columnValue >= Value,
                _ => false
            };
        }
    }

    /// <summary>
    /// SIMD-optimized evaluation for SelectionBitmap.
    /// Processes 8 Int32 values per iteration using AVX2 when available.
    /// </summary>
    public override void Evaluate(RecordBatch batch, ref SelectionBitmap selection, int? endIndex = null)
    {
        var column = batch.Column(ColumnIndex);
        var actualEndIndex = endIndex ?? batch.Length;
        
        // Use EvaluateRange which already supports start/end indices
        EvaluateRange(column, ref selection, 0, actualEndIndex);
    }

    private void EvaluateInt32ArraySimd(Int32Array array, ref SelectionBitmap selection)
    {
        var values = array.Values;
        var length = array.Length;
        var nullBitmap = array.NullBitmapBuffer.Span;
        var hasNulls = array.NullCount > 0;

        // OPTIMIZATION: Filter out nulls in bulk BEFORE predicate evaluation
        // This eliminates per-element null checks in the hot loops below
        if (hasNulls && !nullBitmap.IsEmpty)
        {
            selection.AndWithArrowNullBitmap(nullBitmap);
        }

        // SIMD path: process 8 elements at a time with AVX2
        if (Vector256.IsHardwareAccelerated && length >= 8)
        {
            var compareValue = Vector256.Create(Value);
            ref int valuesRef = ref Unsafe.AsRef(in values[0]);
            int i = 0;
            int vectorEnd = length - (length % 8);
            
            // OPTIMIZATION: Hoist operator switch outside the SIMD loop.
            // Decompose into base operation + negate flag, resolved once.
            var cmp = new ComparisonDecomposition(Operator);
            
            // Prefetch distance: 16 iterations ahead (128 Int32 = 512 bytes = 8 cache lines)
            // This keeps data in L1 cache by the time we process it
            const int prefetchDistance = 128;

            for (; i < vectorEnd; i += 8)
            {
                // Hardware prefetch hint: load data into cache before we need it
                // Prefetch 512 bytes ahead (8 cache lines) to hide memory latency
                if (Sse.IsSupported && i + prefetchDistance < length)
                {
                    unsafe
                    {
                        Sse.Prefetch0((byte*)Unsafe.AsPointer(ref Unsafe.Add(ref valuesRef, i + prefetchDistance)));
                    }
                }
                
                // Load 8 values
                var data = Vector256.LoadUnsafe(ref Unsafe.Add(ref valuesRef, i));
                
                // Perform comparison using pre-resolved decomposition (no per-iteration switch)
                var mask = cmp.Compare(data, compareValue);

                // Apply mask to bitmap (nulls already filtered out)
                ApplyMaskToBitmap(mask, ref selection, i);
            }

            // Scalar tail (nulls already filtered out)
            for (; i < length; i++)
            {
                if (!selection[i]) continue;
                if (!EvaluateScalar(values[i]))
                {
                    selection.Clear(i);
                }
            }
        }
        else
        {
            // Scalar fallback for non-AVX2 systems (nulls already filtered out)
            for (int i = 0; i < length; i++)
            {
                if (!selection[i]) continue;
                if (!EvaluateScalar(values[i]))
                {
                    selection.Clear(i);
                }
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ApplyMaskToBitmap(Vector256<int> mask, ref SelectionBitmap selection, int startIndex)
    {
        // Use MoveMask to convert SIMD mask to 8-bit mask in a single instruction
        // MoveMask extracts the high bit of each 32-bit element (8 elements -> 8 bits)
        // Since comparison produces 0xFFFFFFFF for true, the high bit is 1 for matches
        if (Avx2.IsSupported)
        {
            // Convert int32 mask to byte mask using MoveMask
            var floatMask = mask.AsSingle();
            var byteMask = (byte)Avx.MoveMask(floatMask);
            
            // Apply the mask - this ANDs with existing selection automatically
            // Nulls were already filtered out in bulk, so no need to check here
            selection.AndMask8(startIndex, byteMask);
        }
        else
        {
            // Scalar fallback
            ApplyMaskToBitmapScalar(mask, ref selection, startIndex);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ApplyMaskToBitmapWithNullCheck(Vector256<int> mask, ref SelectionBitmap selection, int startIndex, bool hasNulls, ReadOnlySpan<byte> nullBitmap)
    {
        // Version used by EvaluateRange where bulk null filtering wasn't applied
        if (Avx2.IsSupported)
        {
            var floatMask = mask.AsSingle();
            var byteMask = (byte)Avx.MoveMask(floatMask);
            
            // Handle nulls by clearing those bits from the mask
            if (hasNulls)
            {
                byteMask = ApplyNullMaskVectorized(byteMask, nullBitmap, startIndex);
            }
            
            selection.AndMask8(startIndex, byteMask);
        }
        else
        {
            // Scalar fallback with null check
            for (int j = 0; j < 8; j++)
            {
                var idx = startIndex + j;
                if (!selection[idx]) continue;
                
                if (hasNulls && IsNull(nullBitmap, idx))
                {
                    selection.Clear(idx);
                    continue;
                }
                
                if (mask.GetElement(j) == 0)
                {
                    selection.Clear(idx);
                }
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static byte ApplyNullMaskVectorized(byte mask, ReadOnlySpan<byte> nullBitmap, int startIndex)
    {
        // Extract 8 bits from the Arrow null bitmap
        // Arrow uses LSB first: bit 0 of byte 0 = index 0
        // Null bitmap: 1 = valid, 0 = null
        var byteIndex = startIndex >> 3;  // startIndex / 8
        var bitOffset = startIndex & 7;   // startIndex % 8
        
        byte nullMask;
        if (bitOffset == 0)
        {
            // Aligned case: can read directly
            nullMask = byteIndex < nullBitmap.Length ? nullBitmap[byteIndex] : (byte)0xFF;
        }
        else
        {
            // Unaligned: need to combine two bytes
            var lowByte = byteIndex < nullBitmap.Length ? nullBitmap[byteIndex] : (byte)0xFF;
            var highByte = (byteIndex + 1) < nullBitmap.Length ? nullBitmap[byteIndex + 1] : (byte)0xFF;
            nullMask = (byte)((lowByte >> bitOffset) | (highByte << (8 - bitOffset)));
        }
        
        // AND the masks together: keep bits that are both matched and non-null
        return (byte)(mask & nullMask);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ApplyMaskToBitmapScalar(Vector256<int> mask, ref SelectionBitmap selection, int startIndex)
    {
        // Scalar fallback for non-AVX2 systems
        // Nulls were already filtered out in bulk, so no need to check here
        for (int j = 0; j < 8; j++)
        {
            var idx = startIndex + j;
            if (!selection[idx]) continue;
            
            if (mask.GetElement(j) == 0)
            {
                selection.Clear(idx);
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsNull(ReadOnlySpan<byte> nullBitmap, int index)
    {
        if (nullBitmap.IsEmpty) return false;
        return (nullBitmap[index >> 3] & (1 << (index & 7))) == 0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool EvaluateScalar(int columnValue)
    {
        return Operator switch
        {
            ComparisonOperator.Equal => columnValue == Value,
            ComparisonOperator.NotEqual => columnValue != Value,
            ComparisonOperator.LessThan => columnValue < Value,
            ComparisonOperator.LessThanOrEqual => columnValue <= Value,
            ComparisonOperator.GreaterThan => columnValue > Value,
            ComparisonOperator.GreaterThanOrEqual => columnValue >= Value,
            _ => false
        };
    }

    protected override bool EvaluateSingle(IArrowArray column, int index)
    {
        if (column.IsNull(index)) return false;
        var columnValue = RunLengthEncodedArrayBuilder.GetInt32Value(column, index);
        return EvaluateScalar(columnValue);
    }

    /// <summary>
    /// Evaluates this predicate for a range of rows with SIMD optimization.
    /// Thread-safe for non-overlapping ranges.
    /// </summary>
    public override void EvaluateRange(IArrowArray column, ref SelectionBitmap selection, int startIndex, int endIndex)
    {
        if (column is Int32Array int32Array)
        {
            EvaluateInt32RangeSimd(int32Array, ref selection, startIndex, endIndex);
            return;
        }

        // Fallback to scalar path
        base.EvaluateRange(column, ref selection, startIndex, endIndex);
    }

    /// <summary>
    /// Tests if a chunk can potentially contain matching rows based on zone map min/max values.
    /// </summary>
    public override bool MayContainMatches(ColumnZoneMapData? zoneMapData, int chunkIndex)
    {
        if (zoneMapData == null || zoneMapData.Type != ZoneMapType.Int32)
            return true; // No zone map available, must evaluate

        if (zoneMapData.AllNulls[chunkIndex])
            return false; // All nulls, predicate will fail

        var min = (int)zoneMapData.Mins[chunkIndex];
        var max = (int)zoneMapData.Maxs[chunkIndex];

        // Test if the chunk's [min, max] range overlaps with the predicate's acceptable range
        return Operator switch
        {
            ComparisonOperator.Equal => Value >= min && Value <= max,
            ComparisonOperator.NotEqual => true, // Can't skip - might have both matching and non-matching values
            ComparisonOperator.LessThan => min < Value, // Skip if all values >= Value
            ComparisonOperator.LessThanOrEqual => min <= Value,
            ComparisonOperator.GreaterThan => max > Value, // Skip if all values <= Value
            ComparisonOperator.GreaterThanOrEqual => max >= Value,
            _ => true
        };
    }

    private void EvaluateInt32RangeSimd(Int32Array array, ref SelectionBitmap selection, int startIndex, int endIndex)
    {
        var values = array.Values;
        var nullBitmap = array.NullBitmapBuffer.Span;
        var hasNulls = array.NullCount > 0;
        int i = startIndex;

        // SIMD path: process 8 elements at a time with AVX2
        if (Vector256.IsHardwareAccelerated && (endIndex - startIndex) >= 8)
        {
            var compareValue = Vector256.Create(Value);
            ref int valuesRef = ref Unsafe.AsRef(in values[0]);
            
            // OPTIMIZATION: Hoist operator switch outside the SIMD loop.
            var cmp = new ComparisonDecomposition(Operator);
            
            // Align to 8-element boundary from startIndex
            int vectorStart = ((startIndex + 7) >> 3) << 3; // Round up to next 8
            int vectorEnd = (endIndex >> 3) << 3; // Round down to 8

            // Scalar head (before aligned start)
            for (; i < vectorStart && i < endIndex; i++)
            {
                if (!selection[i]) continue;
                if (hasNulls && IsNull(nullBitmap, i))
                {
                    selection.Clear(i);
                    continue;
                }
                if (!EvaluateScalar(values[i]))
                {
                    selection.Clear(i);
                }
            }

            // SIMD middle
            for (; i < vectorEnd; i += 8)
            {
                var data = Vector256.LoadUnsafe(ref Unsafe.Add(ref valuesRef, i));
                
                // Perform comparison using pre-resolved decomposition (no per-iteration switch)
                var mask = cmp.Compare(data, compareValue);

                // For range evaluation, we still need per-element null checks
                // since bulk filtering wasn't applied (parallel execution path)
                ApplyMaskToBitmapWithNullCheck(mask, ref selection, i, hasNulls, nullBitmap);
            }
        }

        // Scalar tail
        for (; i < endIndex; i++)
        {
            if (!selection[i]) continue;
            if (hasNulls && IsNull(nullBitmap, i))
            {
                selection.Clear(i);
                continue;
            }
            if (!EvaluateScalar(values[i]))
            {
                selection.Clear(i);
            }
        }
    }
}

/// <summary>
/// Predicate for comparing a Double column against a constant value.
/// Uses SIMD vectorization for bulk comparison when processing primitive DoubleArrays.
/// </summary>
public sealed class DoubleComparisonPredicate : ColumnPredicate
{
    public override string ColumnName { get; }
    public override int ColumnIndex { get; }
    public ComparisonOperator Operator { get; }
    public double Value { get; }

    public DoubleComparisonPredicate(string columnName, int columnIndex, ComparisonOperator op, double value)
    {
        ColumnName = columnName;
        ColumnIndex = columnIndex;
        Operator = op;
        Value = value;
    }

    public override void Evaluate(RecordBatch batch, Span<bool> selection)
    {
        var column = batch.Column(ColumnIndex);
        var length = batch.Length;

        for (int i = 0; i < length; i++)
        {
            if (!selection[i]) continue;

            if (column.IsNull(i))
            {
                selection[i] = false;
                continue;
            }

            var columnValue = RunLengthEncodedArrayBuilder.GetDoubleValue(column, i);
            selection[i] = Operator switch
            {
                ComparisonOperator.Equal => columnValue == Value,
                ComparisonOperator.NotEqual => columnValue != Value,
                ComparisonOperator.LessThan => columnValue < Value,
                ComparisonOperator.LessThanOrEqual => columnValue <= Value,
                ComparisonOperator.GreaterThan => columnValue > Value,
                ComparisonOperator.GreaterThanOrEqual => columnValue >= Value,
                _ => false
            };
        }
    }

    /// <summary>
    /// SIMD-optimized evaluation for SelectionBitmap.
    /// Processes 4 double values per iteration using AVX2 when available.
    /// </summary>
    public override void Evaluate(RecordBatch batch, ref SelectionBitmap selection, int? endIndex = null)
    {
        var column = batch.Column(ColumnIndex);
        var actualEndIndex = endIndex ?? batch.Length;
        
        // Use EvaluateRange which already supports start/end indices
        EvaluateRange(column, ref selection, 0, actualEndIndex);
    }

    private void EvaluateDoubleArraySimd(DoubleArray array, ref SelectionBitmap selection)
    {
        var values = array.Values;
        var length = array.Length;
        var nullBitmap = array.NullBitmapBuffer.Span;
        var hasNulls = array.NullCount > 0;

        // OPTIMIZATION: Filter out nulls in bulk BEFORE predicate evaluation
        // This eliminates per-element null checks in the hot loops below
        if (hasNulls && !nullBitmap.IsEmpty)
        {
            selection.AndWithArrowNullBitmap(nullBitmap);
        }

        // SIMD path: process 4 elements at a time with AVX2 (256-bit = 4 doubles)
        if (Vector256.IsHardwareAccelerated && length >= 4)
        {
            var compareValue = Vector256.Create(Value);
            ref double valuesRef = ref Unsafe.AsRef(in values[0]);
            int i = 0;
            int vectorEnd = length - (length % 4);
            
            // OPTIMIZATION: Hoist operator switch outside the SIMD loop.
            var cmp = new ComparisonDecomposition(Operator);
            
            // Prefetch distance: 16 iterations ahead (64 Double = 512 bytes = 8 cache lines)
            const int prefetchDistance = 64;

            for (; i < vectorEnd; i += 4)
            {
                // Hardware prefetch hint: load data into cache before we need it
                if (Sse.IsSupported && i + prefetchDistance < length)
                {
                    unsafe
                    {
                        Sse.Prefetch0((byte*)Unsafe.AsPointer(ref Unsafe.Add(ref valuesRef, i + prefetchDistance)));
                    }
                }
                
                // Load 4 values
                var data = Vector256.LoadUnsafe(ref Unsafe.Add(ref valuesRef, i));
                
                // Perform comparison using pre-resolved decomposition (no per-iteration switch)
                var mask = cmp.Compare(data, compareValue);

                // Apply mask to bitmap (nulls already filtered out)
                ApplyDoubleMaskToBitmap(mask, ref selection, i);
            }

            // Scalar tail (nulls already filtered out)
            for (; i < length; i++)
            {
                if (!selection[i]) continue;
                if (!EvaluateScalar(values[i]))
                {
                    selection.Clear(i);
                }
            }
        }
        else
        {
            // Scalar fallback for non-AVX2 systems (nulls already filtered out)
            for (int i = 0; i < length; i++)
            {
                if (!selection[i]) continue;
                if (!EvaluateScalar(values[i]))
                {
                    selection.Clear(i);
                }
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ApplyDoubleMaskToBitmap(Vector256<double> mask, ref SelectionBitmap selection, int startIndex)
    {
        // Use MoveMask to convert SIMD mask to 4-bit mask
        // For doubles, we get 4 bits (one per 64-bit element)
        // Nulls were already filtered out in bulk, so no need to check here
        if (Avx.IsSupported)
        {
            var byteMask = (byte)Avx.MoveMask(mask);
            
            // Apply the 4-bit mask - AndMask4 automatically ANDs with existing
            selection.AndMask4(startIndex, byteMask);
        }
        else
        {
            // Scalar fallback (nulls already filtered out)
            for (int j = 0; j < 4; j++)
            {
                var idx = startIndex + j;
                if (!selection[idx]) continue;
                
                if (BitConverter.DoubleToInt64Bits(mask.GetElement(j)) == 0)
                {
                    selection.Clear(idx);
                }
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ApplyDoubleMaskToBitmapWithNullCheck(Vector256<double> mask, ref SelectionBitmap selection, int startIndex, bool hasNulls, ReadOnlySpan<byte> nullBitmap)
    {
        // Version used by EvaluateRange where bulk null filtering wasn't applied
        if (Avx.IsSupported)
        {
            var byteMask = (byte)Avx.MoveMask(mask);
            
            // Handle nulls - extract 4 bits from Arrow null bitmap
            if (hasNulls)
            {
                byteMask = ApplyNullMask4Vectorized(byteMask, nullBitmap, startIndex);
            }
            
            selection.AndMask4(startIndex, byteMask);
        }
        else
        {
            // Scalar fallback with null check
            for (int j = 0; j < 4; j++)
            {
                var idx = startIndex + j;
                if (!selection[idx]) continue;
                
                if (hasNulls && IsNull(nullBitmap, idx))
                {
                    selection.Clear(idx);
                    continue;
                }
                
                if (BitConverter.DoubleToInt64Bits(mask.GetElement(j)) == 0)
                {
                    selection.Clear(idx);
                }
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static byte ApplyNullMask4Vectorized(byte mask, ReadOnlySpan<byte> nullBitmap, int startIndex)
    {
        // Extract 4 bits from the Arrow null bitmap
        var byteIndex = startIndex >> 3;
        var bitOffset = startIndex & 7;
        
        byte nullMask;
        if (bitOffset <= 4)
        {
            // Can get all 4 bits from one byte
            nullMask = byteIndex < nullBitmap.Length ? (byte)(nullBitmap[byteIndex] >> bitOffset) : (byte)0x0F;
        }
        else
        {
            // Need to combine two bytes
            var lowByte = byteIndex < nullBitmap.Length ? nullBitmap[byteIndex] : (byte)0xFF;
            var highByte = (byteIndex + 1) < nullBitmap.Length ? nullBitmap[byteIndex + 1] : (byte)0xFF;
            nullMask = (byte)((lowByte >> bitOffset) | (highByte << (8 - bitOffset)));
        }
        
        return (byte)(mask & (nullMask & 0x0F));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsNull(ReadOnlySpan<byte> nullBitmap, int index)
    {
        if (nullBitmap.IsEmpty) return false;
        return (nullBitmap[index >> 3] & (1 << (index & 7))) == 0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool EvaluateScalar(double columnValue)
    {
        return Operator switch
        {
            ComparisonOperator.Equal => columnValue == Value,
            ComparisonOperator.NotEqual => columnValue != Value,
            ComparisonOperator.LessThan => columnValue < Value,
            ComparisonOperator.LessThanOrEqual => columnValue <= Value,
            ComparisonOperator.GreaterThan => columnValue > Value,
            ComparisonOperator.GreaterThanOrEqual => columnValue >= Value,
            _ => false
        };
    }

    protected override bool EvaluateSingle(IArrowArray column, int index)
    {
        if (column.IsNull(index)) return false;
        var columnValue = RunLengthEncodedArrayBuilder.GetDoubleValue(column, index);
        return EvaluateScalar(columnValue);
    }

    /// <summary>
    /// Evaluates this predicate for a range of rows with SIMD optimization.
    /// Thread-safe for non-overlapping ranges.
    /// </summary>
    public override void EvaluateRange(IArrowArray column, ref SelectionBitmap selection, int startIndex, int endIndex)
    {
        if (column is DoubleArray doubleArray)
        {
            EvaluateDoubleRangeSimd(doubleArray, ref selection, startIndex, endIndex);
            return;
        }

        // Fallback to scalar path
        base.EvaluateRange(column, ref selection, startIndex, endIndex);
    }

    /// <summary>
    /// Tests if a chunk can potentially contain matching rows based on zone map min/max values.
    /// </summary>
    public override bool MayContainMatches(ColumnZoneMapData? zoneMapData, int chunkIndex)
    {
        if (zoneMapData == null || zoneMapData.Type != ZoneMapType.Double)
            return true; // No zone map available, must evaluate

        if (zoneMapData.AllNulls[chunkIndex])
            return false; // All nulls, predicate will fail

        var min = (double)zoneMapData.Mins[chunkIndex];
        var max = (double)zoneMapData.Maxs[chunkIndex];

        // Test if the chunk's [min, max] range overlaps with the predicate's acceptable range
        return Operator switch
        {
            ComparisonOperator.Equal => Value >= min && Value <= max,
            ComparisonOperator.NotEqual => true, // Can't skip - might have both matching and non-matching values
            ComparisonOperator.LessThan => min < Value,
            ComparisonOperator.LessThanOrEqual => min <= Value,
            ComparisonOperator.GreaterThan => max > Value,
            ComparisonOperator.GreaterThanOrEqual => max >= Value,
            _ => true
        };
    }

    private void EvaluateDoubleRangeSimd(DoubleArray array, ref SelectionBitmap selection, int startIndex, int endIndex)
    {
        var values = array.Values;
        var nullBitmap = array.NullBitmapBuffer.Span;
        var hasNulls = array.NullCount > 0;
        int i = startIndex;

        // SIMD path: process 4 elements at a time with AVX2 (256-bit = 4 doubles)
        if (Vector256.IsHardwareAccelerated && (endIndex - startIndex) >= 4)
        {
            var compareValue = Vector256.Create(Value);
            ref double valuesRef = ref Unsafe.AsRef(in values[0]);
            
            // OPTIMIZATION: Hoist operator switch outside the SIMD loop.
            var cmp = new ComparisonDecomposition(Operator);
            
            // Align to 4-element boundary from startIndex
            int vectorStart = ((startIndex + 3) >> 2) << 2; // Round up to next 4
            int vectorEnd = (endIndex >> 2) << 2; // Round down to 4

            // Scalar head (before aligned start)
            for (; i < vectorStart && i < endIndex; i++)
            {
                if (!selection[i]) continue;
                if (hasNulls && IsNull(nullBitmap, i))
                {
                    selection.Clear(i);
                    continue;
                }
                if (!EvaluateScalar(values[i]))
                {
                    selection.Clear(i);
                }
            }

            // SIMD middle
            for (; i < vectorEnd; i += 4)
            {
                var data = Vector256.LoadUnsafe(ref Unsafe.Add(ref valuesRef, i));
                
                // Perform comparison using pre-resolved decomposition (no per-iteration switch)
                var mask = cmp.Compare(data, compareValue);

                // For range evaluation, we still need per-element null checks
                // since bulk filtering wasn't applied (parallel execution path)
                ApplyDoubleMaskToBitmapWithNullCheck(mask, ref selection, i, hasNulls, nullBitmap);
            }
        }

        // Scalar tail
        for (; i < endIndex; i++)
        {
            if (!selection[i]) continue;
            if (hasNulls && IsNull(nullBitmap, i))
            {
                selection.Clear(i);
                continue;
            }
            if (!EvaluateScalar(values[i]))
            {
                selection.Clear(i);
            }
        }
    }
}

/// <summary>
/// Predicate for comparing a Decimal column against a constant value.
/// </summary>
public sealed class DecimalComparisonPredicate : ColumnPredicate
{
    public override string ColumnName { get; }
    public override int ColumnIndex { get; }
    public ComparisonOperator Operator { get; }
    public decimal Value { get; }

    public DecimalComparisonPredicate(string columnName, int columnIndex, ComparisonOperator op, decimal value)
    {
        ColumnName = columnName;
        ColumnIndex = columnIndex;
        Operator = op;
        Value = value;
    }

    public override void Evaluate(RecordBatch batch, Span<bool> selection)
    {
        var column = batch.Column(ColumnIndex);
        var length = batch.Length;

        for (int i = 0; i < length; i++)
        {
            if (!selection[i]) continue;

            if (column.IsNull(i))
            {
                selection[i] = false;
                continue;
            }

            var columnValue = RunLengthEncodedArrayBuilder.GetDecimalValue(column, i);
            selection[i] = Operator switch
            {
                ComparisonOperator.Equal => columnValue == Value,
                ComparisonOperator.NotEqual => columnValue != Value,
                ComparisonOperator.LessThan => columnValue < Value,
                ComparisonOperator.LessThanOrEqual => columnValue <= Value,
                ComparisonOperator.GreaterThan => columnValue > Value,
                ComparisonOperator.GreaterThanOrEqual => columnValue >= Value,
                _ => false
            };
        }
    }

    protected override bool EvaluateSingle(IArrowArray column, int index)
    {
        if (column.IsNull(index)) return false;
        var columnValue = RunLengthEncodedArrayBuilder.GetDecimalValue(column, index);
        return Operator switch
        {
            ComparisonOperator.Equal => columnValue == Value,
            ComparisonOperator.NotEqual => columnValue != Value,
            ComparisonOperator.LessThan => columnValue < Value,
            ComparisonOperator.LessThanOrEqual => columnValue <= Value,
            ComparisonOperator.GreaterThan => columnValue > Value,
            ComparisonOperator.GreaterThanOrEqual => columnValue >= Value,
            _ => false
        };
    }

    /// <summary>
    /// Tests if a chunk can potentially contain matching rows based on zone map min/max values.
    /// </summary>
    public override bool MayContainMatches(ColumnZoneMapData? zoneMapData, int chunkIndex)
    {
        if (zoneMapData == null || zoneMapData.Type != ZoneMapType.Decimal)
            return true; // No zone map available, must evaluate

        if (zoneMapData.AllNulls[chunkIndex])
            return false; // All nulls, predicate will fail

        var min = (decimal)zoneMapData.Mins[chunkIndex];
        var max = (decimal)zoneMapData.Maxs[chunkIndex];

        // Test if the chunk's [min, max] range overlaps with the predicate's acceptable range
        return Operator switch
        {
            ComparisonOperator.Equal => Value >= min && Value <= max,
            ComparisonOperator.NotEqual => true, // Can't skip - might have both matching and non-matching values
            ComparisonOperator.LessThan => min < Value,
            ComparisonOperator.LessThanOrEqual => min <= Value,
            ComparisonOperator.GreaterThan => max > Value,
            ComparisonOperator.GreaterThanOrEqual => max >= Value,
            _ => true
        };
    }
}

/// <summary>
/// Predicate for string equality comparison.
/// </summary>
public sealed class StringEqualityPredicate : ColumnPredicate
{
    public override string ColumnName { get; }
    public override int ColumnIndex { get; }
    public string? Value { get; }
    public bool Negate { get; }
    public StringComparison Comparison { get; }

    public StringEqualityPredicate(string columnName, int columnIndex, string? value, bool negate = false, StringComparison comparison = StringComparison.Ordinal)
    {
        ColumnName = columnName;
        ColumnIndex = columnIndex;
        Value = value;
        Negate = negate;
        Comparison = comparison;
    }

    public override void Evaluate(RecordBatch batch, Span<bool> selection)
    {
        var column = batch.Column(ColumnIndex);

        // Fast path for dictionary-encoded strings
        if (column is DictionaryArray dictArray && dictArray.Dictionary is StringArray)
        {
            EvaluateDictionaryEncoded(dictArray, selection);
            return;
        }

        // Standard path for primitive string arrays
        var length = batch.Length;

        for (int i = 0; i < length; i++)
        {
            if (!selection[i]) continue;

            if (column.IsNull(i))
            {
                // null == null is true, null == "something" is false
                var result = Value is null;
                selection[i] = Negate ? !result : result;
                continue;
            }

            if (Value is null)
            {
                // non-null != null
                selection[i] = Negate;
                continue;
            }

            var columnValue = RunLengthEncodedArrayBuilder.GetStringValue(column, i);
            var matches = string.Equals(columnValue, Value, Comparison);
            selection[i] = Negate ? !matches : matches;
        }
    }

    /// <summary>
    /// Optimized evaluation for dictionary-encoded string columns.
    /// Evaluates the predicate once per unique dictionary entry (O(unique_values))
    /// instead of once per row (O(rows)), then broadcasts results using index lookups.
    /// </summary>
    /// <remarks>
    /// For a 1M row column with 100 unique values, this reduces string comparisons
    /// from 1,000,000 to 100 - a 10,000x reduction in comparison operations.
    /// </remarks>
    private void EvaluateDictionaryEncoded(DictionaryArray dictArray, Span<bool> selection)
    {
        var dictionary = (StringArray)dictArray.Dictionary;
        var indices = dictArray.Indices;
        var dictionaryLength = dictionary.Length;

        // Step 1: Evaluate predicate for each unique dictionary entry (O(unique_values))
        // Use stackalloc for small dictionaries, ArrayPool for larger ones
        bool[]? pooledArray = null;
        Span<bool> dictionaryResults = dictionaryLength <= 1024
            ? stackalloc bool[dictionaryLength]
            : (pooledArray = ArrayPool<bool>.Shared.Rent(dictionaryLength)).AsSpan(0, dictionaryLength);

        try
        {
            for (int dictIndex = 0; dictIndex < dictionaryLength; dictIndex++)
            {
                var dictValue = dictionary.GetString(dictIndex);
                var matches = string.Equals(dictValue, Value, Comparison);
                dictionaryResults[dictIndex] = Negate ? !matches : matches;
            }

            // Step 2: Broadcast results to rows based on their dictionary indices (O(rows))
            // Optimized path: direct array access for common index types
            int length = dictArray.Length;
            
            switch (indices)
            {
                case UInt8Array uint8Indices:
                    EvaluateDictionaryEncodedUInt8(uint8Indices, dictionaryResults, selection, length, dictArray);
                    break;
                case UInt16Array uint16Indices:
                    EvaluateDictionaryEncodedUInt16(uint16Indices, dictionaryResults, selection, length, dictArray);
                    break;
                case Int32Array int32Indices:
                    EvaluateDictionaryEncodedInt32(int32Indices, dictionaryResults, selection, length, dictArray);
                    break;
                default:
                    // Fallback for other index types
                    for (int i = 0; i < length; i++)
                    {
                        if (!selection[i]) continue;

                        if (dictArray.IsNull(i))
                        {
                            var result = Value is null;
                            selection[i] = Negate ? !result : result;
                        }
                        else
                        {
                            var dictIndex = DictionaryArrayHelper.GetDictionaryIndex(indices, i);
                            selection[i] = dictionaryResults[dictIndex];
                        }
                    }
                    break;
            }
        }
        finally
        {
            if (pooledArray != null)
            {
                ArrayPool<bool>.Shared.Return(pooledArray);
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void EvaluateDictionaryEncodedUInt8(UInt8Array indices, Span<bool> dictionaryResults, Span<bool> selection, int length, DictionaryArray dictArray)
    {
        var indicesSpan = indices.Values;
        for (int i = 0; i < length; i++)
        {
            if (!selection[i]) continue;

            if (dictArray.IsNull(i))
            {
                var result = Value is null;
                selection[i] = Negate ? !result : result;
            }
            else
            {
                selection[i] = dictionaryResults[indicesSpan[i]];
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void EvaluateDictionaryEncodedUInt16(UInt16Array indices, Span<bool> dictionaryResults, Span<bool> selection, int length, DictionaryArray dictArray)
    {
        var indicesSpan = indices.Values;
        for (int i = 0; i < length; i++)
        {
            if (!selection[i]) continue;

            if (dictArray.IsNull(i))
            {
                var result = Value is null;
                selection[i] = Negate ? !result : result;
            }
            else
            {
                selection[i] = dictionaryResults[indicesSpan[i]];
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void EvaluateDictionaryEncodedInt32(Int32Array indices, Span<bool> dictionaryResults, Span<bool> selection, int length, DictionaryArray dictArray)
    {
        var indicesSpan = indices.Values;
        for (int i = 0; i < length; i++)
        {
            if (!selection[i]) continue;

            if (dictArray.IsNull(i))
            {
                var result = Value is null;
                selection[i] = Negate ? !result : result;
            }
            else
            {
                selection[i] = dictionaryResults[indicesSpan[i]];
            }
        }
    }

    protected override bool EvaluateSingle(IArrowArray column, int index)
    {
        if (column.IsNull(index))
        {
            var result = Value is null;
            return Negate ? !result : result;
        }
        if (Value is null) return Negate;
        var columnValue = RunLengthEncodedArrayBuilder.GetStringValue(column, index);
        var matches = string.Equals(columnValue, Value, Comparison);
        return Negate ? !matches : matches;
    }
}

/// <summary>
/// Predicate for string operations like Contains, StartsWith, EndsWith.
/// </summary>
public sealed class StringOperationPredicate : ColumnPredicate
{
    public override string ColumnName { get; }
    public override int ColumnIndex { get; }
    public string Pattern { get; }
    public StringOperation Operation { get; }
    public StringComparison Comparison { get; }

    public StringOperationPredicate(string columnName, int columnIndex, string pattern, StringOperation operation, StringComparison comparison = StringComparison.Ordinal)
    {
        ColumnName = columnName;
        ColumnIndex = columnIndex;
        Pattern = pattern ?? throw new ArgumentNullException(nameof(pattern));
        Operation = operation;
        Comparison = comparison;
    }

    public override void Evaluate(RecordBatch batch, Span<bool> selection)
    {
        var column = batch.Column(ColumnIndex);

        // Fast path for dictionary-encoded strings
        if (column is DictionaryArray dictArray && dictArray.Dictionary is StringArray)
        {
            EvaluateDictionaryEncoded(dictArray, selection);
            return;
        }

        // Standard path for primitive string arrays
        var length = batch.Length;

        for (int i = 0; i < length; i++)
        {
            if (!selection[i]) continue;

            if (column.IsNull(i))
            {
                selection[i] = false;
                continue;
            }

            var columnValue = RunLengthEncodedArrayBuilder.GetStringValue(column, i);
            if (columnValue is null)
            {
                selection[i] = false;
                continue;
            }
            selection[i] = Operation switch
            {
                StringOperation.Contains => columnValue.Contains(Pattern, Comparison),
                StringOperation.StartsWith => columnValue.StartsWith(Pattern, Comparison),
                StringOperation.EndsWith => columnValue.EndsWith(Pattern, Comparison),
                _ => false
            };
        }
    }

    /// <summary>
    /// Optimized evaluation for dictionary-encoded string columns.
    /// Evaluates the predicate once per unique dictionary entry (O(unique_values))
    /// instead of once per row (O(rows)), then broadcasts results using index lookups.
    /// </summary>
    private void EvaluateDictionaryEncoded(DictionaryArray dictArray, Span<bool> selection)
    {
        var dictionary = (StringArray)dictArray.Dictionary;
        var indices = dictArray.Indices;
        var dictionaryLength = dictionary.Length;

        // Step 1: Evaluate predicate for each unique dictionary entry
        bool[]? pooledArray = null;
        Span<bool> dictionaryResults = dictionaryLength <= 1024
            ? stackalloc bool[dictionaryLength]
            : (pooledArray = ArrayPool<bool>.Shared.Rent(dictionaryLength)).AsSpan(0, dictionaryLength);

        try
        {
            for (int dictIndex = 0; dictIndex < dictionaryLength; dictIndex++)
            {
                var dictValue = dictionary.GetString(dictIndex);
                if (dictValue is null)
                {
                    dictionaryResults[dictIndex] = false;
                    continue;
                }

                dictionaryResults[dictIndex] = Operation switch
                {
                    StringOperation.Contains => dictValue.Contains(Pattern, Comparison),
                    StringOperation.StartsWith => dictValue.StartsWith(Pattern, Comparison),
                    StringOperation.EndsWith => dictValue.EndsWith(Pattern, Comparison),
                    _ => false
                };
            }

            // Step 2: Broadcast results to rows - optimized with direct array access
            int length = dictArray.Length;
            
            switch (indices)
            {
                case UInt8Array uint8Indices:
                    {
                        var indicesSpan = uint8Indices.Values;
                        for (int i = 0; i < length; i++)
                        {
                            if (!selection[i]) continue;
                            selection[i] = dictArray.IsNull(i) ? false : dictionaryResults[indicesSpan[i]];
                        }
                    }
                    break;
                case UInt16Array uint16Indices:
                    {
                        var indicesSpan = uint16Indices.Values;
                        for (int i = 0; i < length; i++)
                        {
                            if (!selection[i]) continue;
                            selection[i] = dictArray.IsNull(i) ? false : dictionaryResults[indicesSpan[i]];
                        }
                    }
                    break;
                case Int32Array int32Indices:
                    {
                        var indicesSpan = int32Indices.Values;
                        for (int i = 0; i < length; i++)
                        {
                            if (!selection[i]) continue;
                            selection[i] = dictArray.IsNull(i) ? false : dictionaryResults[indicesSpan[i]];
                        }
                    }
                    break;
                default:
                    // Fallback
                    for (int i = 0; i < length; i++)
                    {
                        if (!selection[i]) continue;

                        if (dictArray.IsNull(i))
                        {
                            selection[i] = false;
                        }
                        else
                        {
                            var dictIndex = DictionaryArrayHelper.GetDictionaryIndex(indices, i);
                            selection[i] = dictionaryResults[dictIndex];
                        }
                    }
                    break;
            }
        }
        finally
        {
            if (pooledArray != null)
            {
                ArrayPool<bool>.Shared.Return(pooledArray);
            }
        }
    }

    protected override bool EvaluateSingle(IArrowArray column, int index)
    {
        if (column.IsNull(index)) return false;
        var columnValue = RunLengthEncodedArrayBuilder.GetStringValue(column, index);
        if (columnValue is null) return false;
        return Operation switch
        {
            StringOperation.Contains => columnValue.Contains(Pattern, Comparison),
            StringOperation.StartsWith => columnValue.StartsWith(Pattern, Comparison),
            StringOperation.EndsWith => columnValue.EndsWith(Pattern, Comparison),
            _ => false
        };
    }
}

/// <summary>
/// String operations supported in predicates.
/// </summary>
public enum StringOperation
{
    Contains,
    StartsWith,
    EndsWith
}

/// <summary>
/// Predicate for boolean column evaluation.
/// </summary>
public sealed class BooleanPredicate : ColumnPredicate
{
    public override string ColumnName { get; }
    public override int ColumnIndex { get; }
    public bool ExpectedValue { get; }

    public BooleanPredicate(string columnName, int columnIndex, bool expectedValue = true)
    {
        ColumnName = columnName;
        ColumnIndex = columnIndex;
        ExpectedValue = expectedValue;
    }

    public override void Evaluate(RecordBatch batch, Span<bool> selection)
    {
        var column = batch.Column(ColumnIndex);
        var length = batch.Length;

        if (column is Apache.Arrow.BooleanArray boolArray)
        {
            // Per-element null check (legacy path)
            for (int i = 0; i < length; i++)
            {
                if (!selection[i]) continue;

                if (boolArray.IsNull(i))
                {
                    selection[i] = false;
                    continue;
                }

                var value = boolArray.GetValue(i);
                selection[i] = value == ExpectedValue;
            }
        }
    }

    /// <summary>
    /// SIMD-optimized evaluation for SelectionBitmap with bulk null filtering.
    /// </summary>
    public override void Evaluate(RecordBatch batch, ref SelectionBitmap selection, int? endIndex = null)
    {
        var column = batch.Column(ColumnIndex);
        var actualEndIndex = endIndex ?? batch.Length;
        
        if (column is Apache.Arrow.BooleanArray boolArray)
        {
            EvaluateBooleanArrayWithNullFiltering(boolArray, ref selection, 0, actualEndIndex);
        }
        else
        {
            // Fallback to base implementation
            base.Evaluate(batch, ref selection, endIndex);
        }
    }

    private void EvaluateBooleanArrayWithNullFiltering(Apache.Arrow.BooleanArray boolArray, ref SelectionBitmap selection, int startIndex, int endIndex)
    {
        var length = endIndex - startIndex;
        var hasNulls = boolArray.NullCount > 0;
        var nullBitmap = boolArray.NullBitmapBuffer.Span;
        var valueBitmap = boolArray.ValueBuffer.Span;

        // OPTIMIZATION: Filter out nulls in bulk BEFORE value evaluation
        // This eliminates per-element null checks in the loops below
        if (hasNulls && !nullBitmap.IsEmpty)
        {
            selection.AndWithArrowNullBitmap(nullBitmap);
        }

        // Now filter by ExpectedValue
        // Arrow BooleanArray uses a bitmap for values: 1 = true, 0 = false
        // We need to AND the selection with either the value bitmap (ExpectedValue=true)
        // or its complement (ExpectedValue=false)
        if (!valueBitmap.IsEmpty)
        {
            if (ExpectedValue)
            {
                // Keep only rows where value is true
                selection.AndWithArrowNullBitmap(valueBitmap);
            }
            else
            {
                // Keep only rows where value is false
                // This requires ANDing with the complement of the value bitmap
                AndWithArrowBitmapComplement(ref selection, valueBitmap, length);
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void AndWithArrowBitmapComplement(ref SelectionBitmap selection, ReadOnlySpan<byte> arrowBitmap, int length)
    {
        // AND the selection bitmap with the complement of the Arrow bitmap
        // This is equivalent to keeping only rows where the Arrow bit is 0 (false)
        
        int blockIndex = 0;
        int byteIndex = 0;
        
        // Process in 64-bit blocks (8 bytes from Arrow bitmap = 1 ulong block)
        while (blockIndex < selection.BlockCount && byteIndex + 7 < arrowBitmap.Length)
        {
            // Read 8 bytes from Arrow bitmap and combine into ulong
            ulong bitmap = arrowBitmap[byteIndex]
                | ((ulong)arrowBitmap[byteIndex + 1] << 8)
                | ((ulong)arrowBitmap[byteIndex + 2] << 16)
                | ((ulong)arrowBitmap[byteIndex + 3] << 24)
                | ((ulong)arrowBitmap[byteIndex + 4] << 32)
                | ((ulong)arrowBitmap[byteIndex + 5] << 40)
                | ((ulong)arrowBitmap[byteIndex + 6] << 48)
                | ((ulong)arrowBitmap[byteIndex + 7] << 56);

            // Complement the bitmap (keep rows where bit is 0)
            ulong complementBitmap = ~bitmap;
            
            selection.AndBlock(blockIndex, complementBitmap);
            
            byteIndex += 8;
            blockIndex++;
        }

        // Handle tail bytes
        if (blockIndex < selection.BlockCount && byteIndex < arrowBitmap.Length)
        {
            ulong bitmap = 0;
            int remainingBytes = Math.Min(arrowBitmap.Length - byteIndex, 8);
            
            for (int i = 0; i < remainingBytes; i++)
            {
                bitmap |= (ulong)arrowBitmap[byteIndex + i] << (i * 8);
            }
            
            // Fill remaining bits with 0s (will become 1s after complement = keep those rows)
            int remainingBits = length - (blockIndex * 64);
            if (remainingBits < 64)
            {
                ulong validMask = (1UL << remainingBits) - 1;
                bitmap &= validMask; // Zero out bits beyond length
            }
            
            ulong complementBitmap = ~bitmap;
            
            // For tail, we need to preserve bits beyond our length
            if (remainingBits < 64)
            {
                ulong validMask = (1UL << remainingBits) - 1;
                complementBitmap = (complementBitmap & validMask) | ~validMask;
            }
            
            selection.AndBlock(blockIndex, complementBitmap);
        }
    }

    protected override bool EvaluateSingle(IArrowArray column, int index)
    {
        if (column is not Apache.Arrow.BooleanArray boolArray) return false;
        if (boolArray.IsNull(index)) return false;
        return boolArray.GetValue(index) == ExpectedValue;
    }
}

/// <summary>
/// Predicate for null checking.
/// </summary>
public sealed class IsNullPredicate : ColumnPredicate
{
    public override string ColumnName { get; }
    public override int ColumnIndex { get; }
    public bool CheckForNull { get; }

    public IsNullPredicate(string columnName, int columnIndex, bool checkForNull = true)
    {
        ColumnName = columnName;
        ColumnIndex = columnIndex;
        CheckForNull = checkForNull;
    }

    public override void Evaluate(RecordBatch batch, Span<bool> selection)
    {
        var column = batch.Column(ColumnIndex);
        var length = batch.Length;

        for (int i = 0; i < length; i++)
        {
            if (!selection[i]) continue;

            var isNull = column.IsNull(i);
            selection[i] = CheckForNull ? isNull : !isNull;
        }
    }

    protected override bool EvaluateSingle(IArrowArray column, int index)
    {
        var isNull = column.IsNull(index);
        return CheckForNull ? isNull : !isNull;
    }
}

/// <summary>
/// A composite predicate that combines multiple predicates with AND logic.
/// </summary>
public sealed class AndPredicate : ColumnPredicate
{
    public override string ColumnName => string.Join(" AND ", _predicates.Select(p => p.ColumnName));
    
    /// <summary>
    /// AndPredicate doesn't operate on a single column, so ColumnIndex is not applicable.
    /// </summary>
    public override int ColumnIndex => -1;
    
    private readonly List<ColumnPredicate> _predicates;

    public IReadOnlyList<ColumnPredicate> Predicates => _predicates;

    public AndPredicate(IEnumerable<ColumnPredicate> predicates)
    {
        _predicates = predicates.ToList();
    }

    public override void Evaluate(RecordBatch batch, Span<bool> selection)
    {
        foreach (var predicate in _predicates)
        {
            predicate.Evaluate(batch, selection);
        }
    }

    protected override bool EvaluateSingle(IArrowArray column, int index)
    {
        // AndPredicate doesn't use a single column, so this is not applicable
        throw new NotSupportedException("AndPredicate.EvaluateSingle is not supported. Use EvaluateSingleRow instead.");
    }

    /// <summary>
    /// For AndPredicate, we need to evaluate each sub-predicate against its respective column.
    /// This override handles the composite nature of the predicate.
    /// </summary>
    public override bool EvaluateSingleRow(IArrowArray column, int index)
    {
        // Note: For AndPredicate, 'column' is not used directly because each
        // sub-predicate operates on its own column. The caller must ensure
        // that sub-predicates have their columns pre-resolved.
        // This is handled in FusedAggregator which passes the correct columns.
        throw new NotSupportedException(
            "AndPredicate should be decomposed into individual predicates for fused execution. " +
            "Each sub-predicate should be evaluated against its specific column.");
    }
}
