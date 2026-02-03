using Apache.Arrow;

namespace FrozenArrow.Query;

/// <summary>
/// Executes aggregate operations in parallel across data chunks with final reduction.
/// </summary>
internal static class ParallelAggregator
{
    /// <summary>
    /// Minimum row count to enable parallel aggregation.
    /// Below this threshold, sequential aggregation is faster.
    /// </summary>
    private const int ParallelThreshold = 50_000;

    /// <summary>
    /// Default chunk size for parallel aggregation (64K rows).
    /// Larger than predicate chunks because aggregation has less per-row overhead.
    /// </summary>
    private const int DefaultChunkSize = 65_536;

    #region Parallel Sum

    /// <summary>
    /// Computes Sum in parallel using partial sums with reduction.
    /// </summary>
    public static object ExecuteSumParallel(
        IArrowArray column,
        ref SelectionBitmap selection,
        Type resultType,
        ParallelQueryOptions? options = null)
    {
        var length = column.Length;
        var enableParallel = options?.EnableParallelExecution ?? true;
        var threshold = options?.ParallelThreshold ?? ParallelThreshold;

        if (!enableParallel || length < threshold)
        {
            return ColumnAggregator.ExecuteSum(column, ref selection, resultType);
        }

        return column switch
        {
            Int32Array int32Array => ConvertResult(SumInt32Parallel(int32Array, ref selection, options), resultType),
            Int64Array int64Array => ConvertResult(SumInt64Parallel(int64Array, ref selection, options), resultType),
            DoubleArray doubleArray => ConvertResult(SumDoubleParallel(doubleArray, ref selection, options), resultType),
            Decimal128Array decimalArray => ConvertResult(SumDecimalParallel(decimalArray, ref selection, options), resultType),
            _ => ColumnAggregator.ExecuteSum(column, ref selection, resultType)
        };
    }

    private static long SumInt32Parallel(Int32Array array, ref SelectionBitmap selection, ParallelQueryOptions? options)
    {
        var length = array.Length;
        var chunkSize = options?.ChunkSize ?? DefaultChunkSize;
        var chunkCount = (length + chunkSize - 1) / chunkSize;
        var partialSums = new long[chunkCount];

        var selectionBuffer = selection.Buffer!;
        var parallelOptions = CreateParallelOptions(options);
        
        // Pre-apply null bitmap to selection in bulk - eliminates per-element null checks
        // This is a single O(n/64) pass that enables branchless aggregation loops
        var hasNulls = array.NullCount > 0;
        if (hasNulls)
        {
            selection.AndWithNullBitmap(array.NullBitmapBuffer.Span, hasNulls);
        }

        Parallel.For(0, chunkCount, parallelOptions, chunkIndex =>
        {
            var startRow = chunkIndex * chunkSize;
            var endRow = Math.Min(startRow + chunkSize, length);
            
            // Use block-based iteration with nulls pre-applied (no per-element null checks)
            partialSums[chunkIndex] = BlockBasedAggregator.SumInt32BlockBased(
                array, selectionBuffer, startRow, endRow, nullsPreApplied: hasNulls);
        });

        long total = 0;
        for (int i = 0; i < chunkCount; i++) total += partialSums[i];
        return total;
    }

    private static long SumInt64Parallel(Int64Array array, ref SelectionBitmap selection, ParallelQueryOptions? options)
    {
        var length = array.Length;
        var chunkSize = options?.ChunkSize ?? DefaultChunkSize;
        var chunkCount = (length + chunkSize - 1) / chunkSize;
        var partialSums = new long[chunkCount];

        var selectionBuffer = selection.Buffer!;
        var parallelOptions = CreateParallelOptions(options);
        
        // Pre-apply null bitmap to selection - bulk O(n/64) vs per-element O(n)
        var hasNulls = array.NullCount > 0;
        if (hasNulls)
        {
            selection.AndWithNullBitmap(array.NullBitmapBuffer.Span, hasNulls);
        }

        Parallel.For(0, chunkCount, parallelOptions, chunkIndex =>
        {
            var startRow = chunkIndex * chunkSize;
            var endRow = Math.Min(startRow + chunkSize, length);
            
            partialSums[chunkIndex] = BlockBasedAggregator.SumInt64BlockBased(
                array, selectionBuffer, startRow, endRow);
        });

        long total = 0;
        for (int i = 0; i < chunkCount; i++) total += partialSums[i];
        return total;
    }

    private static double SumDoubleParallel(DoubleArray array, ref SelectionBitmap selection, ParallelQueryOptions? options)
    {
        var length = array.Length;
        var chunkSize = options?.ChunkSize ?? DefaultChunkSize;
        var chunkCount = (length + chunkSize - 1) / chunkSize;
        var partialSums = new double[chunkCount];

        var selectionBuffer = selection.Buffer!;
        var parallelOptions = CreateParallelOptions(options);
        
        // Pre-apply null bitmap to selection - bulk O(n/64) vs per-element O(n)
        var hasNulls = array.NullCount > 0;
        if (hasNulls)
        {
            selection.AndWithNullBitmap(array.NullBitmapBuffer.Span, hasNulls);
        }

        Parallel.For(0, chunkCount, parallelOptions, chunkIndex =>
        {
            var startRow = chunkIndex * chunkSize;
            var endRow = Math.Min(startRow + chunkSize, length);
            
            // Use block-based iteration with nulls pre-applied
            partialSums[chunkIndex] = BlockBasedAggregator.SumDoubleBlockBased(
                array, selectionBuffer, startRow, endRow, nullsPreApplied: hasNulls);
        });

        double total = 0;
        for (int i = 0; i < chunkCount; i++) total += partialSums[i];
        return total;
    }

    private static decimal SumDecimalParallel(Decimal128Array array, ref SelectionBitmap selection, ParallelQueryOptions? options)
    {
        var length = array.Length;
        var chunkSize = options?.ChunkSize ?? DefaultChunkSize;
        var chunkCount = (length + chunkSize - 1) / chunkSize;
        var partialSums = new decimal[chunkCount];

        var selectionBuffer = selection.Buffer!;
        var parallelOptions = CreateParallelOptions(options);
        
        // Pre-apply null bitmap to selection - bulk O(n/64) vs per-element O(n)
        var hasNulls = array.NullCount > 0;
        if (hasNulls)
        {
            selection.AndWithNullBitmap(array.NullBitmapBuffer.Span, hasNulls);
        }

        Parallel.For(0, chunkCount, parallelOptions, chunkIndex =>
        {
            var startRow = chunkIndex * chunkSize;
            var endRow = Math.Min(startRow + chunkSize, length);
            
            // Use block-based iteration for efficient sparse access
            partialSums[chunkIndex] = BlockBasedAggregator.SumDecimalBlockBased(
                array, selectionBuffer, startRow, endRow, nullsPreApplied: hasNulls);
        });

        decimal total = 0;
        for (int i = 0; i < chunkCount; i++) total += partialSums[i];
        return total;
    }

    #endregion

    #region Parallel Average

    /// <summary>
    /// Computes Average in parallel using partial (sum, count) pairs with reduction.
    /// </summary>
    public static object ExecuteAverageParallel(
        IArrowArray column,
        ref SelectionBitmap selection,
        Type resultType,
        ParallelQueryOptions? options = null)
    {
        var length = column.Length;
        var enableParallel = options?.EnableParallelExecution ?? true;
        var threshold = options?.ParallelThreshold ?? ParallelThreshold;

        if (!enableParallel || length < threshold)
        {
            return ColumnAggregator.ExecuteAverage(column, ref selection, resultType);
        }

        return column switch
        {
            Int32Array int32Array => ConvertResult(AverageInt32Parallel(int32Array, ref selection, options), resultType),
            Int64Array int64Array => ConvertResult(AverageInt64Parallel(int64Array, ref selection, options), resultType),
            DoubleArray doubleArray => ConvertResult(AverageDoubleParallel(doubleArray, ref selection, options), resultType),
            Decimal128Array decimalArray => ConvertResult(AverageDecimalParallel(decimalArray, ref selection, options), resultType),
            _ => ColumnAggregator.ExecuteAverage(column, ref selection, resultType)
        };
    }

    private static double AverageInt32Parallel(Int32Array array, ref SelectionBitmap selection, ParallelQueryOptions? options)
    {
        var length = array.Length;
        var chunkSize = options?.ChunkSize ?? DefaultChunkSize;
        var chunkCount = (length + chunkSize - 1) / chunkSize;
        var partialSums = new long[chunkCount];
        var partialCounts = new int[chunkCount];

        var selectionBuffer = selection.Buffer!;
        var parallelOptions = CreateParallelOptions(options);
        
        // Pre-apply null bitmap to selection - bulk O(n/64) vs per-element O(n)
        var hasNulls = array.NullCount > 0;
        if (hasNulls)
        {
            selection.AndWithNullBitmap(array.NullBitmapBuffer.Span, hasNulls);
        }

        Parallel.For(0, chunkCount, parallelOptions, chunkIndex =>
        {
            var startRow = chunkIndex * chunkSize;
            var endRow = Math.Min(startRow + chunkSize, length);
            
            // Use block-based iteration with nulls pre-applied
            var (sum, count) = BlockBasedAggregator.SumAndCountInt32BlockBased(
                array, selectionBuffer, startRow, endRow, nullsPreApplied: hasNulls);
            partialSums[chunkIndex] = sum;
            partialCounts[chunkIndex] = count;
        });

        long totalSum = 0;
        int totalCount = 0;
        for (int i = 0; i < chunkCount; i++)
        {
            totalSum += partialSums[i];
            totalCount += partialCounts[i];
        }
        return totalCount > 0 ? (double)totalSum / totalCount : 0;
    }

    private static double AverageInt64Parallel(Int64Array array, ref SelectionBitmap selection, ParallelQueryOptions? options)
    {
        var length = array.Length;
        var chunkSize = options?.ChunkSize ?? DefaultChunkSize;
        var chunkCount = (length + chunkSize - 1) / chunkSize;
        var partialSums = new long[chunkCount];
        var partialCounts = new int[chunkCount];

        var selectionBuffer = selection.Buffer!;
        var parallelOptions = CreateParallelOptions(options);
        
        // Pre-apply null bitmap to selection - bulk O(n/64) vs per-element O(n)
        var hasNulls = array.NullCount > 0;
        if (hasNulls)
        {
            selection.AndWithNullBitmap(array.NullBitmapBuffer.Span, hasNulls);
        }

        Parallel.For(0, chunkCount, parallelOptions, chunkIndex =>
        {
            var startRow = chunkIndex * chunkSize;
            var endRow = Math.Min(startRow + chunkSize, length);
            
            // Use block-based iteration with nulls pre-applied
            partialSums[chunkIndex] = BlockBasedAggregator.SumInt64BlockBased(
                array, selectionBuffer, startRow, endRow);
            
            // For count, we need to count set bits in this range
            partialCounts[chunkIndex] = CountSetBitsInRange(selectionBuffer, startRow, endRow);
        });

        long totalSum = 0;
        int totalCount = 0;
        for (int i = 0; i < chunkCount; i++)
        {
            totalSum += partialSums[i];
            totalCount += partialCounts[i];
        }
        return totalCount > 0 ? (double)totalSum / totalCount : 0;
    }

    private static double AverageDoubleParallel(DoubleArray array, ref SelectionBitmap selection, ParallelQueryOptions? options)
    {
        var length = array.Length;
        var chunkSize = options?.ChunkSize ?? DefaultChunkSize;
        var chunkCount = (length + chunkSize - 1) / chunkSize;
        var partialSums = new double[chunkCount];
        var partialCounts = new int[chunkCount];

        var selectionBuffer = selection.Buffer!;
        var parallelOptions = CreateParallelOptions(options);
        
        // Pre-apply null bitmap to selection - bulk O(n/64) vs per-element O(n)
        var hasNulls = array.NullCount > 0;
        if (hasNulls)
        {
            selection.AndWithNullBitmap(array.NullBitmapBuffer.Span, hasNulls);
        }

        Parallel.For(0, chunkCount, parallelOptions, chunkIndex =>
        {
            var startRow = chunkIndex * chunkSize;
            var endRow = Math.Min(startRow + chunkSize, length);
            
            // Use block-based iteration with nulls pre-applied
            var (sum, count) = BlockBasedAggregator.SumAndCountDoubleBlockBased(
                array, selectionBuffer, startRow, endRow, nullsPreApplied: hasNulls);
            partialSums[chunkIndex] = sum;
            partialCounts[chunkIndex] = count;
        });

        double totalSum = 0;
        int totalCount = 0;
        for (int i = 0; i < chunkCount; i++)
        {
            totalSum += partialSums[i];
            totalCount += partialCounts[i];
        }
        return totalCount > 0 ? totalSum / totalCount : 0;
    }

    private static decimal AverageDecimalParallel(Decimal128Array array, ref SelectionBitmap selection, ParallelQueryOptions? options)
    {
        var length = array.Length;
        var chunkSize = options?.ChunkSize ?? DefaultChunkSize;
        var chunkCount = (length + chunkSize - 1) / chunkSize;
        var partialSums = new decimal[chunkCount];
        var partialCounts = new int[chunkCount];

        var selectionBuffer = selection.Buffer!;
        var parallelOptions = CreateParallelOptions(options);
        
        // Pre-apply null bitmap to selection - bulk O(n/64) vs per-element O(n)
        var hasNulls = array.NullCount > 0;
        if (hasNulls)
        {
            selection.AndWithNullBitmap(array.NullBitmapBuffer.Span, hasNulls);
        }

        Parallel.For(0, chunkCount, parallelOptions, chunkIndex =>
        {
            var startRow = chunkIndex * chunkSize;
            var endRow = Math.Min(startRow + chunkSize, length);
            
            // Use block-based iteration with nulls pre-applied
            var (sum, count) = BlockBasedAggregator.SumAndCountDecimalBlockBased(
                array, selectionBuffer, startRow, endRow, nullsPreApplied: hasNulls);
            partialSums[chunkIndex] = sum;
            partialCounts[chunkIndex] = count;
        });

        decimal totalSum = 0;
        int totalCount = 0;
        for (int i = 0; i < chunkCount; i++)
        {
            totalSum += partialSums[i];
            totalCount += partialCounts[i];
        }
        return totalCount > 0 ? totalSum / totalCount : 0;
    }
    
    /// <summary>
    /// Counts set bits in a range of the selection buffer.
    /// </summary>
    private static int CountSetBitsInRange(ulong[] buffer, int startRow, int endRow)
    {
        int startBlock = startRow >> 6;
        int endBlock = (endRow - 1) >> 6;
        int count = 0;
        
        for (int blockIndex = startBlock; blockIndex <= endBlock; blockIndex++)
        {
            ulong block = buffer[blockIndex];
            if (block == 0) continue;
            
            int blockStartBit = blockIndex << 6;
            
            // Mask off bits before startRow
            if (blockStartBit < startRow)
            {
                int bitsToSkip = startRow - blockStartBit;
                block &= ~((1UL << bitsToSkip) - 1);
            }
            
            // Mask off bits at or after endRow
            int blockEndBit = blockStartBit + 64;
            if (blockEndBit > endRow)
            {
                int bitsToKeep = endRow - blockStartBit;
                if (bitsToKeep < 64)
                {
                    block &= (1UL << bitsToKeep) - 1;
                }
            }
            
            count += System.Numerics.BitOperations.PopCount(block);
        }
        
        return count;
    }

    #endregion

    #region Parallel Min

    /// <summary>
    /// Computes Min in parallel using partial minimums with reduction.
    /// </summary>
    public static object ExecuteMinParallel(
        IArrowArray column,
        ref SelectionBitmap selection,
        Type resultType,
        ParallelQueryOptions? options = null)
    {
        var length = column.Length;
        var enableParallel = options?.EnableParallelExecution ?? true;
        var threshold = options?.ParallelThreshold ?? ParallelThreshold;

        if (!enableParallel || length < threshold)
        {
            return ColumnAggregator.ExecuteMin(column, ref selection, resultType);
        }

        return column switch
        {
            Int32Array int32Array => ConvertResult(MinInt32Parallel(int32Array, ref selection, options), resultType),
            Int64Array int64Array => ConvertResult(MinInt64Parallel(int64Array, ref selection, options), resultType),
            DoubleArray doubleArray => ConvertResult(MinDoubleParallel(doubleArray, ref selection, options), resultType),
            Decimal128Array decimalArray => ConvertResult(MinDecimalParallel(decimalArray, ref selection, options), resultType),
            _ => ColumnAggregator.ExecuteMin(column, ref selection, resultType)
        };
    }

    private static int MinInt32Parallel(Int32Array array, ref SelectionBitmap selection, ParallelQueryOptions? options)
    {
        var length = array.Length;
        var chunkSize = options?.ChunkSize ?? DefaultChunkSize;
        var chunkCount = (length + chunkSize - 1) / chunkSize;
        var partialMins = new int[chunkCount];
        var hasValues = new bool[chunkCount];

        for (int i = 0; i < chunkCount; i++) partialMins[i] = int.MaxValue;

        var selectionBuffer = selection.Buffer!;
        var parallelOptions = CreateParallelOptions(options);

        Parallel.For(0, chunkCount, parallelOptions, chunkIndex =>
        {
            var startRow = chunkIndex * chunkSize;
            var endRow = Math.Min(startRow + chunkSize, length);
            
            // Use block-based iteration for efficient sparse access
            var (min, hasValue) = BlockBasedAggregator.MinInt32BlockBased(
                array, selectionBuffer, startRow, endRow);
            partialMins[chunkIndex] = min;
            hasValues[chunkIndex] = hasValue;
        });

        int globalMin = int.MaxValue;
        bool hasAnyValue = false;
        for (int i = 0; i < chunkCount; i++)
        {
            if (hasValues[i] && partialMins[i] < globalMin)
            {
                globalMin = partialMins[i];
                hasAnyValue = true;
            }
        }

        if (!hasAnyValue) throw new InvalidOperationException("Sequence contains no elements.");
        return globalMin;
    }

    private static long MinInt64Parallel(Int64Array array, ref SelectionBitmap selection, ParallelQueryOptions? options)
    {
        var length = array.Length;
        var chunkSize = options?.ChunkSize ?? DefaultChunkSize;
        var chunkCount = (length + chunkSize - 1) / chunkSize;
        var partialMins = new long[chunkCount];
        var hasValues = new bool[chunkCount];

        for (int i = 0; i < chunkCount; i++) partialMins[i] = long.MaxValue;

        var selectionBuffer = selection.Buffer!;
        var parallelOptions = CreateParallelOptions(options);

        Parallel.For(0, chunkCount, parallelOptions, chunkIndex =>
        {
            var startRow = chunkIndex * chunkSize;
            var endRow = Math.Min(startRow + chunkSize, length);
            var values = array.Values;

            long chunkMin = long.MaxValue;
            bool foundValue = false;
            for (int i = startRow; i < endRow; i++)
            {
                if (SelectionBitmap.IsSet(selectionBuffer, i) && !array.IsNull(i))
                {
                    var value = values[i];
                    if (!foundValue || value < chunkMin)
                    {
                        chunkMin = value;
                        foundValue = true;
                    }
                }
            }
            partialMins[chunkIndex] = chunkMin;
            hasValues[chunkIndex] = foundValue;
        });

        long globalMin = long.MaxValue;
        bool hasAnyValue = false;
        for (int i = 0; i < chunkCount; i++)
        {
            if (hasValues[i] && partialMins[i] < globalMin)
            {
                globalMin = partialMins[i];
                hasAnyValue = true;
            }
        }

        if (!hasAnyValue) throw new InvalidOperationException("Sequence contains no elements.");
        return globalMin;
    }

    private static double MinDoubleParallel(DoubleArray array, ref SelectionBitmap selection, ParallelQueryOptions? options)
    {
        var length = array.Length;
        var chunkSize = options?.ChunkSize ?? DefaultChunkSize;
        var chunkCount = (length + chunkSize - 1) / chunkSize;
        var partialMins = new double[chunkCount];
        var hasValues = new bool[chunkCount];

        for (int i = 0; i < chunkCount; i++) partialMins[i] = double.MaxValue;

        var selectionBuffer = selection.Buffer!;
        var parallelOptions = CreateParallelOptions(options);

        Parallel.For(0, chunkCount, parallelOptions, chunkIndex =>
        {
            var startRow = chunkIndex * chunkSize;
            var endRow = Math.Min(startRow + chunkSize, length);
            
            // Use block-based iteration for efficient sparse access
            var (min, hasValue) = BlockBasedAggregator.MinDoubleBlockBased(
                array, selectionBuffer, startRow, endRow);
            partialMins[chunkIndex] = min;
            hasValues[chunkIndex] = hasValue;
        });

        double globalMin = double.MaxValue;
        bool hasAnyValue = false;
        for (int i = 0; i < chunkCount; i++)
        {
            if (hasValues[i] && partialMins[i] < globalMin)
            {
                globalMin = partialMins[i];
                hasAnyValue = true;
            }
        }

        if (!hasAnyValue) throw new InvalidOperationException("Sequence contains no elements.");
        return globalMin;
    }

    private static decimal MinDecimalParallel(Decimal128Array array, ref SelectionBitmap selection, ParallelQueryOptions? options)
    {
        var length = array.Length;
        var chunkSize = options?.ChunkSize ?? DefaultChunkSize;
        var chunkCount = (length + chunkSize - 1) / chunkSize;
        var partialMins = new decimal[chunkCount];
        var hasValues = new bool[chunkCount];

        for (int i = 0; i < chunkCount; i++) partialMins[i] = decimal.MaxValue;

        var selectionBuffer = selection.Buffer!;
        var parallelOptions = CreateParallelOptions(options);

        Parallel.For(0, chunkCount, parallelOptions, chunkIndex =>
        {
            var startRow = chunkIndex * chunkSize;
            var endRow = Math.Min(startRow + chunkSize, length);

            decimal chunkMin = decimal.MaxValue;
            bool foundValue = false;
            for (int i = startRow; i < endRow; i++)
            {
                if (SelectionBitmap.IsSet(selectionBuffer, i) && !array.IsNull(i))
                {
                    var value = array.GetValue(i)!.Value;
                    if (!foundValue || value < chunkMin)
                    {
                        chunkMin = value;
                        foundValue = true;
                    }
                }
            }
            partialMins[chunkIndex] = chunkMin;
            hasValues[chunkIndex] = foundValue;
        });

        decimal globalMin = decimal.MaxValue;
        bool hasAnyValue = false;
        for (int i = 0; i < chunkCount; i++)
        {
            if (hasValues[i] && partialMins[i] < globalMin)
            {
                globalMin = partialMins[i];
                hasAnyValue = true;
            }
        }

        if (!hasAnyValue) throw new InvalidOperationException("Sequence contains no elements.");
        return globalMin;
    }

    #endregion

    #region Parallel Max

    /// <summary>
    /// Computes Max in parallel using partial maximums with reduction.
    /// </summary>
    public static object ExecuteMaxParallel(
        IArrowArray column,
        ref SelectionBitmap selection,
        Type resultType,
        ParallelQueryOptions? options = null)
    {
        var length = column.Length;
        var enableParallel = options?.EnableParallelExecution ?? true;
        var threshold = options?.ParallelThreshold ?? ParallelThreshold;

        if (!enableParallel || length < threshold)
        {
            return ColumnAggregator.ExecuteMax(column, ref selection, resultType);
        }

        return column switch
        {
            Int32Array int32Array => ConvertResult(MaxInt32Parallel(int32Array, ref selection, options), resultType),
            Int64Array int64Array => ConvertResult(MaxInt64Parallel(int64Array, ref selection, options), resultType),
            DoubleArray doubleArray => ConvertResult(MaxDoubleParallel(doubleArray, ref selection, options), resultType),
            Decimal128Array decimalArray => ConvertResult(MaxDecimalParallel(decimalArray, ref selection, options), resultType),
            _ => ColumnAggregator.ExecuteMax(column, ref selection, resultType)
        };
    }

    private static int MaxInt32Parallel(Int32Array array, ref SelectionBitmap selection, ParallelQueryOptions? options)
    {
        var length = array.Length;
        var chunkSize = options?.ChunkSize ?? DefaultChunkSize;
        var chunkCount = (length + chunkSize - 1) / chunkSize;
        var partialMaxs = new int[chunkCount];
        var hasValues = new bool[chunkCount];

        for (int i = 0; i < chunkCount; i++) partialMaxs[i] = int.MinValue;

        var selectionBuffer = selection.Buffer!;
        var parallelOptions = CreateParallelOptions(options);

        Parallel.For(0, chunkCount, parallelOptions, chunkIndex =>
        {
            var startRow = chunkIndex * chunkSize;
            var endRow = Math.Min(startRow + chunkSize, length);
            
            // Use block-based iteration for efficient sparse access
            var (max, hasValue) = BlockBasedAggregator.MaxInt32BlockBased(
                array, selectionBuffer, startRow, endRow);
            partialMaxs[chunkIndex] = max;
            hasValues[chunkIndex] = hasValue;
        });

        int globalMax = int.MinValue;
        bool hasAnyValue = false;
        for (int i = 0; i < chunkCount; i++)
        {
            if (hasValues[i] && partialMaxs[i] > globalMax)
            {
                globalMax = partialMaxs[i];
                hasAnyValue = true;
            }
        }

        if (!hasAnyValue) throw new InvalidOperationException("Sequence contains no elements.");
        return globalMax;
    }

    private static long MaxInt64Parallel(Int64Array array, ref SelectionBitmap selection, ParallelQueryOptions? options)
    {
        var length = array.Length;
        var chunkSize = options?.ChunkSize ?? DefaultChunkSize;
        var chunkCount = (length + chunkSize - 1) / chunkSize;
        var partialMaxs = new long[chunkCount];
        var hasValues = new bool[chunkCount];

        for (int i = 0; i < chunkCount; i++) partialMaxs[i] = long.MinValue;

        var selectionBuffer = selection.Buffer!;
        var parallelOptions = CreateParallelOptions(options);

        Parallel.For(0, chunkCount, parallelOptions, chunkIndex =>
        {
            var startRow = chunkIndex * chunkSize;
            var endRow = Math.Min(startRow + chunkSize, length);
            var values = array.Values;

            long chunkMax = long.MinValue;
            bool foundValue = false;
            for (int i = startRow; i < endRow; i++)
            {
                if (SelectionBitmap.IsSet(selectionBuffer, i) && !array.IsNull(i))
                {
                    var value = values[i];
                    if (!foundValue || value > chunkMax)
                    {
                        chunkMax = value;
                        foundValue = true;
                    }
                }
            }
            partialMaxs[chunkIndex] = chunkMax;
            hasValues[chunkIndex] = foundValue;
        });

        long globalMax = long.MinValue;
        bool hasAnyValue = false;
        for (int i = 0; i < chunkCount; i++)
        {
            if (hasValues[i] && partialMaxs[i] > globalMax)
            {
                globalMax = partialMaxs[i];
                hasAnyValue = true;
            }
        }

        if (!hasAnyValue) throw new InvalidOperationException("Sequence contains no elements.");
        return globalMax;
    }

    private static double MaxDoubleParallel(DoubleArray array, ref SelectionBitmap selection, ParallelQueryOptions? options)
    {
        var length = array.Length;
        var chunkSize = options?.ChunkSize ?? DefaultChunkSize;
        var chunkCount = (length + chunkSize - 1) / chunkSize;
        var partialMaxs = new double[chunkCount];
        var hasValues = new bool[chunkCount];

        for (int i = 0; i < chunkCount; i++) partialMaxs[i] = double.MinValue;

        var selectionBuffer = selection.Buffer!;
        var parallelOptions = CreateParallelOptions(options);

        Parallel.For(0, chunkCount, parallelOptions, chunkIndex =>
        {
            var startRow = chunkIndex * chunkSize;
            var endRow = Math.Min(startRow + chunkSize, length);
            
            // Use block-based iteration for efficient sparse access
            var (max, hasValue) = BlockBasedAggregator.MaxDoubleBlockBased(
                array, selectionBuffer, startRow, endRow);
            partialMaxs[chunkIndex] = max;
            hasValues[chunkIndex] = hasValue;
        });

        double globalMax = double.MinValue;
        bool hasAnyValue = false;
        for (int i = 0; i < chunkCount; i++)
        {
            if (hasValues[i] && partialMaxs[i] > globalMax)
            {
                globalMax = partialMaxs[i];
                hasAnyValue = true;
            }
        }

        if (!hasAnyValue) throw new InvalidOperationException("Sequence contains no elements.");
        return globalMax;
    }

    private static decimal MaxDecimalParallel(Decimal128Array array, ref SelectionBitmap selection, ParallelQueryOptions? options)
    {
        var length = array.Length;
        var chunkSize = options?.ChunkSize ?? DefaultChunkSize;
        var chunkCount = (length + chunkSize - 1) / chunkSize;
        var partialMaxs = new decimal[chunkCount];
        var hasValues = new bool[chunkCount];

        for (int i = 0; i < chunkCount; i++) partialMaxs[i] = decimal.MinValue;

        var selectionBuffer = selection.Buffer!;
        var parallelOptions = CreateParallelOptions(options);

        Parallel.For(0, chunkCount, parallelOptions, chunkIndex =>
        {
            var startRow = chunkIndex * chunkSize;
            var endRow = Math.Min(startRow + chunkSize, length);

            decimal chunkMax = decimal.MinValue;
            bool foundValue = false;
            for (int i = startRow; i < endRow; i++)
            {
                if (SelectionBitmap.IsSet(selectionBuffer, i) && !array.IsNull(i))
                {
                    var value = array.GetValue(i)!.Value;
                    if (!foundValue || value > chunkMax)
                    {
                        chunkMax = value;
                        foundValue = true;
                    }
                }
            }
            partialMaxs[chunkIndex] = chunkMax;
            hasValues[chunkIndex] = foundValue;
        });

        decimal globalMax = decimal.MinValue;
        bool hasAnyValue = false;
        for (int i = 0; i < chunkCount; i++)
        {
            if (hasValues[i] && partialMaxs[i] > globalMax)
            {
                globalMax = partialMaxs[i];
                hasAnyValue = true;
            }
        }

        if (!hasAnyValue) throw new InvalidOperationException("Sequence contains no elements.");
        return globalMax;
    }

    #endregion

    #region Helpers

    private static ParallelOptions CreateParallelOptions(ParallelQueryOptions? options)
    {
        var parallelOptions = new ParallelOptions();
        if (options?.MaxDegreeOfParallelism > 0)
        {
            parallelOptions.MaxDegreeOfParallelism = options.MaxDegreeOfParallelism;
        }
        return parallelOptions;
    }

    private static object ConvertResult(object value, Type targetType)
    {
        if (targetType == typeof(int)) return Convert.ToInt32(value);
        if (targetType == typeof(long)) return Convert.ToInt64(value);
        if (targetType == typeof(double)) return Convert.ToDouble(value);
        if (targetType == typeof(float)) return Convert.ToSingle(value);
        if (targetType == typeof(decimal)) return Convert.ToDecimal(value);
        return value;
    }

    #endregion
}
