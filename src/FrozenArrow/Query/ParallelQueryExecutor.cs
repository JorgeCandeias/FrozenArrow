using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using Apache.Arrow;

namespace FrozenArrow.Query;

/// <summary>
/// Configuration options for parallel query execution.
/// </summary>
public sealed class ParallelQueryOptions
{
    /// <summary>
    /// Default options for parallel query execution.
    /// </summary>
    public static ParallelQueryOptions Default { get; } = new();

    /// <summary>
    /// Gets or sets the minimum number of rows required to enable parallel execution.
    /// Below this threshold, sequential execution is used to avoid parallel overhead.
    /// Default: 10,000 rows.
    /// </summary>
    public int ParallelThreshold { get; set; } = 10_000;

    /// <summary>
    /// Gets or sets the number of rows per chunk for parallel processing.
    /// Larger chunks reduce overhead but may cause load imbalance.
    /// Default: 16,384 rows (optimized for L2 cache).
    /// </summary>
    public int ChunkSize { get; set; } = 16_384;

    /// <summary>
    /// Gets or sets the maximum degree of parallelism.
    /// Default: -1 (use all available processors).
    /// </summary>
    public int MaxDegreeOfParallelism { get; set; } = -1;

    /// <summary>
    /// Gets or sets whether parallel execution is enabled.
    /// Default: true.
    /// </summary>
    public bool EnableParallelExecution { get; set; } = true;
}

/// <summary>
/// Executes query operations in parallel across data chunks.
/// </summary>
internal static class ParallelQueryExecutor
{
    /// <summary>
    /// Evaluates multiple predicates against a record batch in parallel.
    /// Each chunk of rows is processed by all predicates before moving to the next chunk.
    /// Zone maps are used to skip entire chunks when possible.
    /// Predicates are automatically reordered by estimated selectivity for optimal performance.
    /// </summary>
    /// <param name="batch">The Arrow record batch to evaluate.</param>
    /// <param name="selection">The selection bitmap to update.</param>
    /// <param name="predicates">The predicates to evaluate.</param>
    /// <param name="options">Parallel execution options.</param>
    /// <param name="zoneMap">Optional zone map for skip-scanning optimization.</param>
    /// <param name="maxRowToEvaluate">Maximum row index to evaluate (for Take before Where). If null, evaluates all rows.</param>
    public static void EvaluatePredicatesParallel(
        RecordBatch batch,
        ref SelectionBitmap selection,
        IReadOnlyList<ColumnPredicate> predicates,
        ParallelQueryOptions? options = null,
        ZoneMap? zoneMap = null,
        int? maxRowToEvaluate = null)
    {
        options ??= ParallelQueryOptions.Default;
        var rowCount = maxRowToEvaluate ?? batch.Length;

        // Fall back to sequential for small datasets or when disabled
        if (!options.EnableParallelExecution || 
            rowCount < options.ParallelThreshold || 
            predicates.Count == 0)
        {
            EvaluatePredicatesSequential(batch, ref selection, predicates, zoneMap, maxRowToEvaluate);
            return;
        }

        // Reorder predicates by estimated selectivity (most selective first).
        // This reduces the number of rows that subsequent predicates need to evaluate.
        predicates = PredicateReorderer.ReorderBySelectivity(predicates, zoneMap, rowCount);

        var chunkSize = options.ChunkSize;
        var chunkCount = (rowCount + chunkSize - 1) / chunkSize;

        // For single chunk, use sequential
        if (chunkCount == 1)
        {
            EvaluatePredicatesSequential(batch, ref selection, predicates, zoneMap);
            return;
        }

        var parallelOptions = new ParallelOptions();
        if (options.MaxDegreeOfParallelism > 0)
        {
            parallelOptions.MaxDegreeOfParallelism = options.MaxDegreeOfParallelism;
        }

        // Pre-fetch columns for all predicates to avoid repeated lookups
        var columns = new IArrowArray[predicates.Count];
        for (int i = 0; i < predicates.Count; i++)
        {
            columns[i] = batch.Column(predicates[i].ColumnIndex);
        }

        // Pre-fetch zone map data for predicates
        var zoneMapData = new ColumnZoneMapData?[predicates.Count];
        if (zoneMap != null)
        {
            for (int i = 0; i < predicates.Count; i++)
            {
                zoneMap.TryGetColumnZoneMap(predicates[i].ColumnName, out zoneMapData[i]);
            }
        }

        // Extract the underlying buffer for safe parallel access.
        // The buffer (ulong[]) is a reference type that can be safely captured by the lambda.
        // This avoids taking pointers to managed types which could be invalidated by GC.
        var selectionBuffer = selection.Buffer!;
        var selectionLength = selection.Length;

        // Process chunks in parallel
        Parallel.For(0, chunkCount, parallelOptions, chunkIndex =>
        {
            var startRow = chunkIndex * chunkSize;
            var endRow = Math.Min(startRow + chunkSize, rowCount);

            // Zone map skip test: Check if ANY predicate says this chunk can be skipped
            if (CanSkipChunkViaZoneMap(predicates, zoneMapData, chunkIndex))
            {
                // Clear the entire chunk using bulk operation - O(chunkSize/64) instead of O(chunkSize)
                SelectionBitmap.ClearRangeStatic(selectionBuffer, selectionLength, startRow, endRow);
                return; // Skip chunk evaluation
            }

            // Apply each predicate to this chunk using the buffer directly
            // OPTIMIZATION: Use devirtualized dispatch for common predicate types
            for (int predIdx = 0; predIdx < predicates.Count; predIdx++)
            {
                var predicate = predicates[predIdx];
                var column = columns[predIdx];
                
                // Fast-path: Check concrete type and call non-virtual method
                // This eliminates virtual dispatch overhead for 90%+ of predicates
                if (predicate is Int32ComparisonPredicate int32Pred)
                {
                    EvaluateInt32PredicateRange(int32Pred, column, selectionBuffer, startRow, endRow);
                }
                else if (predicate is DoubleComparisonPredicate doublePred)
                {
                    EvaluateDoublePredicateRange(doublePred, column, selectionBuffer, startRow, endRow);
                }
                else
                {
                    // Fallback: virtual call for uncommon predicate types
                    predicate.EvaluateRangeWithBuffer(column, selectionBuffer, startRow, endRow);
                }
            }
        });
    }

    /// <summary>
    /// Checks if a chunk can be skipped based on zone maps.
    /// Returns true only if at least one predicate definitively excludes the chunk.
    /// </summary>
    private static bool CanSkipChunkViaZoneMap(
        IReadOnlyList<ColumnPredicate> predicates,
        ColumnZoneMapData?[] zoneMapData,
        int chunkIndex)
    {
        // A chunk can be skipped if ANY predicate says it cannot possibly contain matches
        for (int i = 0; i < predicates.Count; i++)
        {
            if (!predicates[i].MayContainMatches(zoneMapData[i], chunkIndex))
            {
                return true; // This predicate excludes the chunk
            }
        }
        return false; // At least one predicate might have matches, must evaluate
    }

    /// <summary>
    /// Evaluates predicates sequentially (original behavior).
    /// Also applies predicate reordering for optimal evaluation order.
    /// </summary>
    /// <param name="maxRowToEvaluate">Maximum row index to evaluate (for Take before Where). If null, evaluates all rows.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void EvaluatePredicatesSequential(
        RecordBatch batch,
        ref SelectionBitmap selection,
        IReadOnlyList<ColumnPredicate> predicates,
        ZoneMap? zoneMap = null,
        int? maxRowToEvaluate = null)
    {
        var rowCount = maxRowToEvaluate ?? batch.Length;
        
        // Reorder predicates by estimated selectivity (most selective first)
        predicates = PredicateReorderer.ReorderBySelectivity(predicates, zoneMap, rowCount);
        
        foreach (var predicate in predicates)
        {
            // Evaluate only up to maxRowToEvaluate
            predicate.Evaluate(batch, ref selection, endIndex: rowCount);
        }
    }

    /// <summary>
    /// Devirtualized Int32 predicate evaluation for parallel execution.
    /// This method eliminates virtual dispatch overhead by directly calling
    /// the specialized SIMD implementation for Int32 comparisons.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void EvaluateInt32PredicateRange(
        Int32ComparisonPredicate predicate,
        IArrowArray column,
        ulong[] selectionBuffer,
        int startIndex,
        int endIndex)
    {
        // Cast to Int32Array for SIMD-optimized evaluation
        if (column is Int32Array int32Array)
        {
            EvaluateInt32ArrayRange(predicate, int32Array, selectionBuffer, startIndex, endIndex);
        }
        else
        {
            // Fallback for dictionary-encoded or other array types
            EvaluateInt32GenericRange(predicate, column, selectionBuffer, startIndex, endIndex);
        }
    }

    /// <summary>
    /// SIMD-optimized evaluation for Int32Array ranges.
    /// Processes 8 elements at a time using AVX2 when available.
    /// </summary>
    private static void EvaluateInt32ArrayRange(
        Int32ComparisonPredicate predicate,
        Int32Array array,
        ulong[] selectionBuffer,
        int startIndex,
        int endIndex)
    {
        var values = array.Values;
        var nullBitmap = array.NullBitmapBuffer.Span;
        var hasNulls = array.NullCount > 0;
        int i = startIndex;

        // OPTIMIZATION: Hoist operator switch outside the SIMD loop.
        var cmp = new ComparisonDecomposition(predicate.Operator);

        // SIMD path: process 8 elements at a time with AVX2
        if (Vector256.IsHardwareAccelerated && (endIndex - startIndex) >= 8)
        {
            var compareValue = Vector256.Create(predicate.Value);
            ref int valuesRef = ref Unsafe.AsRef(in values[0]);
            
            int vectorEnd = ((endIndex - startIndex) >> 3) << 3; // Round down to multiple of 8
            vectorEnd += startIndex;
            
            // Prefetch distance: 16 iterations ahead (128 Int32 = 512 bytes = 8 cache lines)
            const int prefetchDistance = 128;

            for (; i < vectorEnd; i += 8)
            {
                // Check selection first - skip if all already filtered
                if (!AnySelected(selectionBuffer, i, 8))
                    continue;

                // Hardware prefetch hint: load data into cache before we need it
                if (Sse.IsSupported && i + prefetchDistance < endIndex)
                {
                    unsafe
                    {
                        Sse.Prefetch0((byte*)Unsafe.AsPointer(ref Unsafe.Add(ref valuesRef, i + prefetchDistance)));
                    }
                }

                var data = Vector256.LoadUnsafe(ref Unsafe.Add(ref valuesRef, i));
                
                // Perform comparison using pre-resolved decomposition (no per-iteration switch)
                var mask = cmp.Compare(data, compareValue);

                // Apply mask with null checks
                if (Avx2.IsSupported)
                {
                    var floatMask = mask.AsSingle();
                    var byteMask = (byte)Avx.MoveMask(floatMask);
                    
                    // Handle nulls
                    if (hasNulls)
                    {
                        byteMask = ApplyNullMask8(byteMask, nullBitmap, i);
                    }
                    
                    // Apply mask to selection buffer
                    AndMask8ToBuffer(selectionBuffer, i, byteMask);
                }
                else
                {
                    // Scalar fallback
                    for (int j = 0; j < 8; j++)
                    {
                        var idx = i + j;
                        if (!SelectionBitmap.IsSet(selectionBuffer, idx)) continue;
                        
                        if (hasNulls && IsNull(nullBitmap, idx))
                        {
                            SelectionBitmap.ClearBit(selectionBuffer, idx);
                            continue;
                        }
                        
                        if (mask.GetElement(j) == 0)
                        {
                            SelectionBitmap.ClearBit(selectionBuffer, idx);
                        }
                    }
                }
            }
        }

        // Scalar tail
        for (; i < endIndex; i++)
        {
            if (!SelectionBitmap.IsSet(selectionBuffer, i)) continue;
            
            if (hasNulls && IsNull(nullBitmap, i))
            {
                SelectionBitmap.ClearBit(selectionBuffer, i);
                continue;
            }
            
            var columnValue = values[i];
            if (!EvaluateScalarInt32(columnValue, predicate.Value, predicate.Operator))
            {
                SelectionBitmap.ClearBit(selectionBuffer, i);
            }
        }
    }

    /// <summary>
    /// Fallback evaluation for non-Int32Array types (dictionary-encoded, etc.).
    /// </summary>
    private static void EvaluateInt32GenericRange(
        Int32ComparisonPredicate predicate,
        IArrowArray column,
        ulong[] selectionBuffer,
        int startIndex,
        int endIndex)
    {
        for (int i = startIndex; i < endIndex; i++)
        {
            if (!SelectionBitmap.IsSet(selectionBuffer, i)) continue;
            
            if (column.IsNull(i))
            {
                SelectionBitmap.ClearBit(selectionBuffer, i);
                continue;
            }
            
            var columnValue = RunLengthEncodedArrayBuilder.GetInt32Value(column, i);
            if (!EvaluateScalarInt32(columnValue, predicate.Value, predicate.Operator))
            {
                SelectionBitmap.ClearBit(selectionBuffer, i);
            }
        }
    }

    /// <summary>
    /// Devirtualized Double predicate evaluation for parallel execution.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void EvaluateDoublePredicateRange(
        DoubleComparisonPredicate predicate,
        IArrowArray column,
        ulong[] selectionBuffer,
        int startIndex,
        int endIndex)
    {
        if (column is DoubleArray doubleArray)
        {
            EvaluateDoubleArrayRange(predicate, doubleArray, selectionBuffer, startIndex, endIndex);
        }
        else
        {
            EvaluateDoubleGenericRange(predicate, column, selectionBuffer, startIndex, endIndex);
        }
    }

    /// <summary>
    /// SIMD-optimized evaluation for DoubleArray ranges.
    /// Processes 4 elements at a time using AVX2.
    /// </summary>
    private static void EvaluateDoubleArrayRange(
        DoubleComparisonPredicate predicate,
        DoubleArray array,
        ulong[] selectionBuffer,
        int startIndex,
        int endIndex)
    {
        var values = array.Values;
        var nullBitmap = array.NullBitmapBuffer.Span;
        var hasNulls = array.NullCount > 0;
        int i = startIndex;

        // OPTIMIZATION: Hoist operator switch outside the SIMD loop.
        var cmp = new ComparisonDecomposition(predicate.Operator);

        // SIMD path: process 4 elements at a time
        if (Vector256.IsHardwareAccelerated && (endIndex - startIndex) >= 4)
        {
            var compareValue = Vector256.Create(predicate.Value);
            ref double valuesRef = ref Unsafe.AsRef(in values[0]);
            
            int vectorEnd = ((endIndex - startIndex) >> 2) << 2;
            vectorEnd += startIndex;
            
            // Prefetch distance: 16 iterations ahead (64 Double = 512 bytes = 8 cache lines)
            const int prefetchDistance = 64;

            for (; i < vectorEnd; i += 4)
            {
                if (!AnySelected(selectionBuffer, i, 4))
                    continue;

                // Hardware prefetch hint: load data into cache before we need it
                if (Sse.IsSupported && i + prefetchDistance < endIndex)
                {
                    unsafe
                    {
                        Sse.Prefetch0((byte*)Unsafe.AsPointer(ref Unsafe.Add(ref valuesRef, i + prefetchDistance)));
                    }
                }

                var data = Vector256.LoadUnsafe(ref Unsafe.Add(ref valuesRef, i));
                
                // Perform comparison using pre-resolved decomposition (no per-iteration switch)
                var mask = cmp.Compare(data, compareValue);

                if (Avx.IsSupported)
                {
                    var byteMask = (byte)Avx.MoveMask(mask);
                    
                    if (hasNulls)
                    {
                        byteMask = ApplyNullMask4(byteMask, nullBitmap, i);
                    }
                    
                    AndMask4ToBuffer(selectionBuffer, i, byteMask);
                }
                else
                {
                    for (int j = 0; j < 4; j++)
                    {
                        var idx = i + j;
                        if (!SelectionBitmap.IsSet(selectionBuffer, idx)) continue;
                        
                        if (hasNulls && IsNull(nullBitmap, idx))
                        {
                            SelectionBitmap.ClearBit(selectionBuffer, idx);
                            continue;
                        }
                        
                        if (BitConverter.DoubleToInt64Bits(mask.GetElement(j)) == 0)
                        {
                            SelectionBitmap.ClearBit(selectionBuffer, idx);
                        }
                    }
                }
            }
        }

        // Scalar tail
        for (; i < endIndex; i++)
        {
            if (!SelectionBitmap.IsSet(selectionBuffer, i)) continue;
            
            if (hasNulls && IsNull(nullBitmap, i))
            {
                SelectionBitmap.ClearBit(selectionBuffer, i);
                continue;
            }
            
            var columnValue = values[i];
            if (!EvaluateScalarDouble(columnValue, predicate.Value, predicate.Operator))
            {
                SelectionBitmap.ClearBit(selectionBuffer, i);
            }
        }
    }

    private static void EvaluateDoubleGenericRange(
        DoubleComparisonPredicate predicate,
        IArrowArray column,
        ulong[] selectionBuffer,
        int startIndex,
        int endIndex)
    {
        for (int i = startIndex; i < endIndex; i++)
        {
            if (!SelectionBitmap.IsSet(selectionBuffer, i)) continue;
            
            if (column.IsNull(i))
            {
                SelectionBitmap.ClearBit(selectionBuffer, i);
                continue;
            }
            
            var columnValue = RunLengthEncodedArrayBuilder.GetDoubleValue(column, i);
            if (!EvaluateScalarDouble(columnValue, predicate.Value, predicate.Operator))
            {
                SelectionBitmap.ClearBit(selectionBuffer, i);
            }
        }
    }

    /// <summary>
    /// Helper: Check if any bits in a range are set (early-exit optimization).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool AnySelected(ulong[] buffer, int startIndex, int count)
    {
        for (int i = 0; i < count; i++)
        {
            if (SelectionBitmap.IsSet(buffer, startIndex + i))
                return true;
        }
        return false;
    }

    /// <summary>
    /// Helper: Apply null mask to 8-bit comparison result.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static byte ApplyNullMask8(byte mask, ReadOnlySpan<byte> nullBitmap, int startIndex)
    {
        var byteIndex = startIndex >> 3;
        var bitOffset = startIndex & 7;
        
        byte nullMask;
        if (bitOffset == 0)
        {
            nullMask = byteIndex < nullBitmap.Length ? nullBitmap[byteIndex] : (byte)0xFF;
        }
        else
        {
            var lowByte = byteIndex < nullBitmap.Length ? nullBitmap[byteIndex] : (byte)0xFF;
            var highByte = (byteIndex + 1) < nullBitmap.Length ? nullBitmap[byteIndex + 1] : (byte)0xFF;
            nullMask = (byte)((lowByte >> bitOffset) | (highByte << (8 - bitOffset)));
        }
        
        return (byte)(mask & nullMask);
    }

    /// <summary>
    /// Helper: Apply null mask to 4-bit comparison result.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static byte ApplyNullMask4(byte mask, ReadOnlySpan<byte> nullBitmap, int startIndex)
    {
        var byteIndex = startIndex >> 3;
        var bitOffset = startIndex & 7;
        
        byte nullMask;
        if (bitOffset <= 4)
        {
            nullMask = byteIndex < nullBitmap.Length ? (byte)(nullBitmap[byteIndex] >> bitOffset) : (byte)0x0F;
        }
        else
        {
            var lowByte = byteIndex < nullBitmap.Length ? nullBitmap[byteIndex] : (byte)0xFF;
            var highByte = (byteIndex + 1) < nullBitmap.Length ? nullBitmap[byteIndex + 1] : (byte)0xFF;
            nullMask = (byte)((lowByte >> bitOffset) | (highByte << (8 - bitOffset)));
        }
        
        return (byte)(mask & (nullMask & 0x0F));
    }

    /// <summary>
    /// Helper: Check if a bit is null in Arrow null bitmap.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsNull(ReadOnlySpan<byte> nullBitmap, int index)
    {
        if (nullBitmap.IsEmpty) return false;
        return (nullBitmap[index >> 3] & (1 << (index & 7))) == 0;
    }

    /// <summary>
    /// Helper: AND 8-bit mask to selection buffer at specified index.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void AndMask8ToBuffer(ulong[] buffer, int startIndex, byte mask)
    {
        var blockIndex = startIndex >> 6;
        var bitOffset = startIndex & 63;
        
        var preserveMask = ~((ulong)0xFF << bitOffset);
        var andMask = (ulong)mask << bitOffset;
        
        buffer[blockIndex] = (buffer[blockIndex] & preserveMask) | (buffer[blockIndex] & andMask);
    }

    /// <summary>
    /// Helper: AND 4-bit mask to selection buffer at specified index.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void AndMask4ToBuffer(ulong[] buffer, int startIndex, byte mask)
    {
        var blockIndex = startIndex >> 6;
        var bitOffset = startIndex & 63;
        
        var preserveMask = ~((ulong)0x0F << bitOffset);
        var andMask = (ulong)(mask & 0x0F) << bitOffset;
        
        buffer[blockIndex] = (buffer[blockIndex] & preserveMask) | (buffer[blockIndex] & andMask);
    }

    /// <summary>
    /// Evaluates a scalar Int32 comparison. Used in scalar tails to avoid per-iteration operator switch.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool EvaluateScalarInt32(int columnValue, int predicateValue, ComparisonOperator op)
    {
        return op switch
        {
            ComparisonOperator.Equal => columnValue == predicateValue,
            ComparisonOperator.NotEqual => columnValue != predicateValue,
            ComparisonOperator.LessThan => columnValue < predicateValue,
            ComparisonOperator.LessThanOrEqual => columnValue <= predicateValue,
            ComparisonOperator.GreaterThan => columnValue > predicateValue,
            ComparisonOperator.GreaterThanOrEqual => columnValue >= predicateValue,
            _ => false
        };
    }

    /// <summary>
    /// Evaluates a scalar Double comparison. Used in scalar tails to avoid per-iteration operator switch.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool EvaluateScalarDouble(double columnValue, double predicateValue, ComparisonOperator op)
    {
        return op switch
        {
            ComparisonOperator.Equal => columnValue == predicateValue,
            ComparisonOperator.NotEqual => columnValue != predicateValue,
            ComparisonOperator.LessThan => columnValue < predicateValue,
            ComparisonOperator.LessThanOrEqual => columnValue <= predicateValue,
            ComparisonOperator.GreaterThan => columnValue > predicateValue,
            ComparisonOperator.GreaterThanOrEqual => columnValue >= predicateValue,
            _ => false
        };
    }
}
