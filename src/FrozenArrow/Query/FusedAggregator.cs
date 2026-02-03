using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;

namespace FrozenArrow.Query;

/// <summary>
/// Executes fused predicate evaluation and aggregation in a single pass.
/// This eliminates the intermediate bitmap materialization for filtered aggregates,
/// reducing memory bandwidth and improving cache efficiency.
/// </summary>
/// <remarks>
/// The fused approach is beneficial when:
/// - There are predicates to evaluate AND a simple aggregate to compute
/// - The data is accessed sequentially (good cache behavior)
/// - We want to avoid the overhead of building and scanning a bitmap
/// 
/// For queries like `.Where(x => x.Age > 30).Sum(x => x.Salary)`, this processes
/// each row exactly once: evaluate predicate, if true accumulate into aggregate.
/// </remarks>
internal static class FusedAggregator
{
    /// <summary>
    /// Minimum row count where fused execution provides benefit.
    /// Below this, the setup overhead may exceed the savings.
    /// </summary>
    private const int FusedThreshold = 1_000;

    /// <summary>
    /// Determines if a query can benefit from fused execution.
    /// </summary>
    public static bool CanUseFusedExecution(QueryPlan plan, int rowCount, RecordBatch batch, Dictionary<string, int> columnIndexMap)
    {
        // Need both predicates and a simple aggregate
        if (plan.ColumnPredicates.Count == 0 || plan.SimpleAggregate is null)
            return false;

        // Don't fuse grouped queries (they need different handling)
        if (plan.IsGroupedQuery)
            return false;

        // Must be worth the setup cost
        if (rowCount < FusedThreshold)
            return false;

        // Check that the aggregate column is a supported primitive type (not DictionaryArray)
        if (plan.SimpleAggregate.ColumnName is not null && 
            columnIndexMap.TryGetValue(plan.SimpleAggregate.ColumnName, out var colIdx))
        {
            var column = batch.Column(colIdx);
            if (column is DictionaryArray)
                return false; // DictionaryArray not yet supported in fused path
        }

        return true;
    }

    /// <summary>
    /// Executes a fused filter+aggregate operation in a single pass.
    /// </summary>
    /// <param name="batch">The Arrow record batch to process.</param>
    /// <param name="predicates">The predicates to evaluate.</param>
    /// <param name="aggregate">The aggregation operation to perform.</param>
    /// <param name="columnIndexMap">Map of column names to indices.</param>
    /// <param name="options">Parallel execution options.</param>
    /// <param name="zoneMap">Optional zone map for skip-scanning optimization.</param>
    public static object ExecuteFused(
        RecordBatch batch,
        IReadOnlyList<ColumnPredicate> predicates,
        SimpleAggregateOperation aggregate,
        Dictionary<string, int> columnIndexMap,
        ParallelQueryOptions? options = null,
        ZoneMap? zoneMap = null)
    {
        // Get the aggregate column
        var aggregateColumnIndex = aggregate.ColumnName is not null && columnIndexMap.TryGetValue(aggregate.ColumnName, out var idx)
            ? idx
            : -1;

        var aggregateColumn = aggregateColumnIndex >= 0 ? batch.Column(aggregateColumnIndex) : null;

        // Pre-fetch predicate columns
        var predicateColumns = new IArrowArray[predicates.Count];
        for (int i = 0; i < predicates.Count; i++)
        {
            predicateColumns[i] = batch.Column(predicates[i].ColumnIndex);
        }

        // Pre-fetch zone map data for predicates (enables chunk skipping)
        var zoneMapData = new ColumnZoneMapData?[predicates.Count];
        if (zoneMap != null)
        {
            for (int i = 0; i < predicates.Count; i++)
            {
                zoneMap.TryGetColumnZoneMap(predicates[i].ColumnName, out zoneMapData[i]);
            }
        }

        var rowCount = batch.Length;
        var enableParallel = options?.EnableParallelExecution ?? true;
        var threshold = options?.ParallelThreshold ?? 50_000;

        // Choose parallel or sequential based on size
        if (enableParallel && rowCount >= threshold)
        {
            return ExecuteFusedParallel(predicates, predicateColumns, aggregate, aggregateColumn, rowCount, options, zoneMapData);
        }

        return ExecuteFusedSequential(predicates, predicateColumns, aggregate, aggregateColumn, rowCount);
    }

    /// <summary>
    /// Sequential fused execution - evaluates predicates and aggregates in one pass.
    /// </summary>
    private static object ExecuteFusedSequential(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        SimpleAggregateOperation aggregate,
        IArrowArray? aggregateColumn,
        int rowCount)
    {
        return aggregate.Operation switch
        {
            AggregationOperation.Count => FusedCountSequential(predicates, predicateColumns, rowCount),
            AggregationOperation.LongCount => (long)FusedCountSequential(predicates, predicateColumns, rowCount),
            AggregationOperation.Sum => FusedSumSequential(predicates, predicateColumns, aggregateColumn!, rowCount, aggregate.ResultType),
            AggregationOperation.Average => FusedAverageSequential(predicates, predicateColumns, aggregateColumn!, rowCount, aggregate.ResultType),
            AggregationOperation.Min => FusedMinSequential(predicates, predicateColumns, aggregateColumn!, rowCount, aggregate.ResultType),
            AggregationOperation.Max => FusedMaxSequential(predicates, predicateColumns, aggregateColumn!, rowCount, aggregate.ResultType),
            _ => throw new NotSupportedException($"Fused execution not supported for {aggregate.Operation}")
        };
    }

    /// <summary>
    /// Parallel fused execution - partitions data and executes fused operations per chunk.
    /// Uses zone maps to skip entire chunks that cannot contain matching rows.
    /// </summary>
    private static object ExecuteFusedParallel(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        SimpleAggregateOperation aggregate,
        IArrowArray? aggregateColumn,
        int rowCount,
        ParallelQueryOptions? options,
        ColumnZoneMapData?[] zoneMapData)
    {
        var chunkSize = options?.ChunkSize ?? 65_536;
        var chunkCount = (rowCount + chunkSize - 1) / chunkSize;

        var parallelOptions = new ParallelOptions();
        if (options?.MaxDegreeOfParallelism > 0)
        {
            parallelOptions.MaxDegreeOfParallelism = options.MaxDegreeOfParallelism;
        }

        return aggregate.Operation switch
        {
            AggregationOperation.Count => FusedCountParallel(predicates, predicateColumns, rowCount, chunkSize, chunkCount, parallelOptions, zoneMapData),
            AggregationOperation.LongCount => (long)FusedCountParallel(predicates, predicateColumns, rowCount, chunkSize, chunkCount, parallelOptions, zoneMapData),
            AggregationOperation.Sum => FusedSumParallel(predicates, predicateColumns, aggregateColumn!, rowCount, chunkSize, chunkCount, parallelOptions, aggregate.ResultType, zoneMapData),
            AggregationOperation.Average => FusedAverageParallel(predicates, predicateColumns, aggregateColumn!, rowCount, chunkSize, chunkCount, parallelOptions, aggregate.ResultType, zoneMapData),
            AggregationOperation.Min => FusedMinParallel(predicates, predicateColumns, aggregateColumn!, rowCount, chunkSize, chunkCount, parallelOptions, aggregate.ResultType, zoneMapData),
            AggregationOperation.Max => FusedMaxParallel(predicates, predicateColumns, aggregateColumn!, rowCount, chunkSize, chunkCount, parallelOptions, aggregate.ResultType, zoneMapData),
            _ => throw new NotSupportedException($"Fused parallel execution not supported for {aggregate.Operation}")
        };
    }

    /// <summary>
    /// Checks if a chunk can be skipped based on zone maps.
    /// Returns true only if at least one predicate definitively excludes the chunk.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
        return false; // All predicates might have matches, must evaluate
    }

    #region Fused Count

    private static int FusedCountSequential(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        int rowCount)
    {
        int count = 0;

        for (int i = 0; i < rowCount; i++)
        {
            if (EvaluateAllPredicates(predicates, predicateColumns, i))
            {
                count++;
            }
        }

        return count;
    }

    private static int FusedCountParallel(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        int rowCount,
        int chunkSize,
        int chunkCount,
        ParallelOptions parallelOptions,
        ColumnZoneMapData?[] zoneMapData)
    {
        var partialCounts = new int[chunkCount];

        Parallel.For(0, chunkCount, parallelOptions, chunkIndex =>
        {
            // Zone map optimization: skip chunks that cannot contain matches
            if (CanSkipChunkViaZoneMap(predicates, zoneMapData, chunkIndex))
            {
                partialCounts[chunkIndex] = 0;
                return;
            }

            var startRow = chunkIndex * chunkSize;
            var endRow = Math.Min(startRow + chunkSize, rowCount);
            int localCount = 0;

            for (int i = startRow; i < endRow; i++)
            {
                if (EvaluateAllPredicates(predicates, predicateColumns, i))
                {
                    localCount++;
                }
            }

            partialCounts[chunkIndex] = localCount;
        });

        int total = 0;
        for (int i = 0; i < chunkCount; i++)
        {
            total += partialCounts[i];
        }
        return total;
    }

    #endregion

    #region Fused Sum

    private static object FusedSumSequential(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        IArrowArray aggregateColumn,
        int rowCount,
        Type resultType)
    {
        return aggregateColumn switch
        {
            Int32Array int32Array => ConvertResult(FusedSumInt32Sequential(predicates, predicateColumns, int32Array, rowCount), resultType),
            Int64Array int64Array => ConvertResult(FusedSumInt64Sequential(predicates, predicateColumns, int64Array, rowCount), resultType),
            DoubleArray doubleArray => ConvertResult(FusedSumDoubleSequential(predicates, predicateColumns, doubleArray, rowCount), resultType),
            Decimal128Array decimalArray => ConvertResult(FusedSumDecimalSequential(predicates, predicateColumns, decimalArray, rowCount), resultType),
            _ => throw new NotSupportedException($"Fused Sum not supported for {aggregateColumn.GetType().Name}")
        };
    }

    private static long FusedSumInt32Sequential(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        Int32Array valueArray,
        int rowCount)
    {
        // Use SIMD-optimized path when available
        return SimdFusedEvaluator.FusedSumInt32Simd(predicates, predicateColumns, valueArray, 0, rowCount);
    }

    private static long FusedSumInt64Sequential(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        Int64Array valueArray,
        int rowCount)
    {
        long sum = 0;
        var values = valueArray.Values;

        for (int i = 0; i < rowCount; i++)
        {
            if (!valueArray.IsNull(i) && EvaluateAllPredicates(predicates, predicateColumns, i))
            {
                sum += values[i];
            }
        }

        return sum;
    }

    private static double FusedSumDoubleSequential(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        DoubleArray valueArray,
        int rowCount)
    {
        // Use SIMD-optimized path when available
        return SimdFusedEvaluator.FusedSumDoubleSimd(predicates, predicateColumns, valueArray, 0, rowCount);
    }

    private static decimal FusedSumDecimalSequential(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        Decimal128Array valueArray,
        int rowCount)
    {
        decimal sum = 0;

        for (int i = 0; i < rowCount; i++)
        {
            if (!valueArray.IsNull(i) && EvaluateAllPredicates(predicates, predicateColumns, i))
            {
                sum += valueArray.GetValue(i)!.Value;
            }
        }

        return sum;
    }

    private static object FusedSumParallel(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        IArrowArray aggregateColumn,
        int rowCount,
        int chunkSize,
        int chunkCount,
        ParallelOptions parallelOptions,
        Type resultType,
        ColumnZoneMapData?[] zoneMapData)
    {
        return aggregateColumn switch
        {
            Int32Array int32Array => ConvertResult(FusedSumInt32Parallel(predicates, predicateColumns, int32Array, rowCount, chunkSize, chunkCount, parallelOptions, zoneMapData), resultType),
            Int64Array int64Array => ConvertResult(FusedSumInt64Parallel(predicates, predicateColumns, int64Array, rowCount, chunkSize, chunkCount, parallelOptions, zoneMapData), resultType),
            DoubleArray doubleArray => ConvertResult(FusedSumDoubleParallel(predicates, predicateColumns, doubleArray, rowCount, chunkSize, chunkCount, parallelOptions, zoneMapData), resultType),
            Decimal128Array decimalArray => ConvertResult(FusedSumDecimalParallel(predicates, predicateColumns, decimalArray, rowCount, chunkSize, chunkCount, parallelOptions, zoneMapData), resultType),
            _ => throw new NotSupportedException($"Fused parallel Sum not supported for {aggregateColumn.GetType().Name}")
        };
    }

    private static unsafe long FusedSumInt32Parallel(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        Int32Array valueArray,
        int rowCount,
        int chunkSize,
        int chunkCount,
        ParallelOptions parallelOptions,
        ColumnZoneMapData?[] zoneMapData)
    {
        var partialSums = new long[chunkCount];

        Parallel.For(0, chunkCount, parallelOptions, chunkIndex =>
        {
            // Zone map optimization: skip chunks that cannot contain matches
            if (CanSkipChunkViaZoneMap(predicates, zoneMapData, chunkIndex))
            {
                partialSums[chunkIndex] = 0;
                return;
            }

            var startRow = chunkIndex * chunkSize;
            var endRow = Math.Min(startRow + chunkSize, rowCount);
            
            // Use SIMD-optimized evaluation
            partialSums[chunkIndex] = SimdFusedEvaluator.FusedSumInt32Simd(
                predicates, predicateColumns, valueArray, startRow, endRow);
        });

        long total = 0;
        for (int i = 0; i < chunkCount; i++)
        {
            total += partialSums[i];
        }
        return total;
    }

    private static unsafe long FusedSumInt64Parallel(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        Int64Array valueArray,
        int rowCount,
        int chunkSize,
        int chunkCount,
        ParallelOptions parallelOptions,
        ColumnZoneMapData?[] zoneMapData)
    {
        var partialSums = new long[chunkCount];
        ref readonly long valuesRef = ref MemoryMarshal.GetReference(valueArray.Values);
        long* valuesPtr = (long*)Unsafe.AsPointer(ref Unsafe.AsRef(in valuesRef));

        Parallel.For(0, chunkCount, parallelOptions, chunkIndex =>
        {
            // Zone map optimization: skip chunks that cannot contain matches
            if (CanSkipChunkViaZoneMap(predicates, zoneMapData, chunkIndex))
            {
                partialSums[chunkIndex] = 0;
                return;
            }

            var startRow = chunkIndex * chunkSize;
            var endRow = Math.Min(startRow + chunkSize, rowCount);
            long localSum = 0;

            for (int i = startRow; i < endRow; i++)
            {
                if (!valueArray.IsNull(i) && EvaluateAllPredicates(predicates, predicateColumns, i))
                {
                    localSum += valuesPtr[i];
                }
            }

            partialSums[chunkIndex] = localSum;
        });

        long total = 0;
        for (int i = 0; i < chunkCount; i++)
        {
            total += partialSums[i];
        }
        return total;
    }

    private static unsafe double FusedSumDoubleParallel(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        DoubleArray valueArray,
        int rowCount,
        int chunkSize,
        int chunkCount,
        ParallelOptions parallelOptions,
        ColumnZoneMapData?[] zoneMapData)
    {
        var partialSums = new double[chunkCount];

        Parallel.For(0, chunkCount, parallelOptions, chunkIndex =>
        {
            // Zone map optimization: skip chunks that cannot contain matches
            if (CanSkipChunkViaZoneMap(predicates, zoneMapData, chunkIndex))
            {
                partialSums[chunkIndex] = 0;
                return;
            }

            var startRow = chunkIndex * chunkSize;
            var endRow = Math.Min(startRow + chunkSize, rowCount);
            
            // Use SIMD-optimized evaluation
            partialSums[chunkIndex] = SimdFusedEvaluator.FusedSumDoubleSimd(
                predicates, predicateColumns, valueArray, startRow, endRow);
        });

        double total = 0;
        for (int i = 0; i < chunkCount; i++)
        {
            total += partialSums[i];
        }
        return total;
    }

    private static decimal FusedSumDecimalParallel(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        Decimal128Array valueArray,
        int rowCount,
        int chunkSize,
        int chunkCount,
        ParallelOptions parallelOptions,
        ColumnZoneMapData?[] zoneMapData)
    {
        var partialSums = new decimal[chunkCount];

        Parallel.For(0, chunkCount, parallelOptions, chunkIndex =>
        {
            // Zone map optimization: skip chunks that cannot contain matches
            if (CanSkipChunkViaZoneMap(predicates, zoneMapData, chunkIndex))
            {
                partialSums[chunkIndex] = 0;
                return;
            }

            var startRow = chunkIndex * chunkSize;
            var endRow = Math.Min(startRow + chunkSize, rowCount);
            decimal localSum = 0;

            for (int i = startRow; i < endRow; i++)
            {
                if (!valueArray.IsNull(i) && EvaluateAllPredicates(predicates, predicateColumns, i))
                {
                    localSum += valueArray.GetValue(i)!.Value;
                }
            }

            partialSums[chunkIndex] = localSum;
        });

        decimal total = 0;
        for (int i = 0; i < chunkCount; i++)
        {
            total += partialSums[i];
        }
        return total;
    }

    #endregion

    #region Fused Average

    private static object FusedAverageSequential(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        IArrowArray aggregateColumn,
        int rowCount,
        Type resultType)
    {
        return aggregateColumn switch
        {
            Int32Array int32Array => ConvertResult(FusedAverageInt32Sequential(predicates, predicateColumns, int32Array, rowCount), resultType),
            Int64Array int64Array => ConvertResult(FusedAverageInt64Sequential(predicates, predicateColumns, int64Array, rowCount), resultType),
            DoubleArray doubleArray => ConvertResult(FusedAverageDoubleSequential(predicates, predicateColumns, doubleArray, rowCount), resultType),
            Decimal128Array decimalArray => ConvertResult(FusedAverageDecimalSequential(predicates, predicateColumns, decimalArray, rowCount), resultType),
            _ => throw new NotSupportedException($"Fused Average not supported for {aggregateColumn.GetType().Name}")
        };
    }

    private static double FusedAverageInt32Sequential(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        Int32Array valueArray,
        int rowCount)
    {
        long sum = 0;
        int count = 0;
        var values = valueArray.Values;

        for (int i = 0; i < rowCount; i++)
        {
            if (!valueArray.IsNull(i) && EvaluateAllPredicates(predicates, predicateColumns, i))
            {
                sum += values[i];
                count++;
            }
        }

        return count > 0 ? (double)sum / count : 0;
    }

    private static double FusedAverageInt64Sequential(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        Int64Array valueArray,
        int rowCount)
    {
        long sum = 0;
        int count = 0;
        var values = valueArray.Values;

        for (int i = 0; i < rowCount; i++)
        {
            if (!valueArray.IsNull(i) && EvaluateAllPredicates(predicates, predicateColumns, i))
            {
                sum += values[i];
                count++;
            }
        }

        return count > 0 ? (double)sum / count : 0;
    }

    private static double FusedAverageDoubleSequential(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        DoubleArray valueArray,
        int rowCount)
    {
        double sum = 0;
        int count = 0;
        var values = valueArray.Values;

        for (int i = 0; i < rowCount; i++)
        {
            if (!valueArray.IsNull(i) && EvaluateAllPredicates(predicates, predicateColumns, i))
            {
                sum += values[i];
                count++;
            }
        }

        return count > 0 ? sum / count : 0;
    }

    private static decimal FusedAverageDecimalSequential(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        Decimal128Array valueArray,
        int rowCount)
    {
        decimal sum = 0;
        int count = 0;

        for (int i = 0; i < rowCount; i++)
        {
            if (!valueArray.IsNull(i) && EvaluateAllPredicates(predicates, predicateColumns, i))
            {
                sum += valueArray.GetValue(i)!.Value;
                count++;
            }
        }

        return count > 0 ? sum / count : 0;
    }

    private static object FusedAverageParallel(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        IArrowArray aggregateColumn,
        int rowCount,
        int chunkSize,
        int chunkCount,
        ParallelOptions parallelOptions,
        Type resultType,
        ColumnZoneMapData?[] zoneMapData)
    {
        return aggregateColumn switch
        {
            Int32Array int32Array => ConvertResult(FusedAverageInt32Parallel(predicates, predicateColumns, int32Array, rowCount, chunkSize, chunkCount, parallelOptions, zoneMapData), resultType),
            Int64Array int64Array => ConvertResult(FusedAverageInt64Parallel(predicates, predicateColumns, int64Array, rowCount, chunkSize, chunkCount, parallelOptions, zoneMapData), resultType),
            DoubleArray doubleArray => ConvertResult(FusedAverageDoubleParallel(predicates, predicateColumns, doubleArray, rowCount, chunkSize, chunkCount, parallelOptions, zoneMapData), resultType),
            Decimal128Array decimalArray => ConvertResult(FusedAverageDecimalParallel(predicates, predicateColumns, decimalArray, rowCount, chunkSize, chunkCount, parallelOptions, zoneMapData), resultType),
            _ => throw new NotSupportedException($"Fused parallel Average not supported for {aggregateColumn.GetType().Name}")
        };
    }

    private static unsafe double FusedAverageInt32Parallel(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        Int32Array valueArray,
        int rowCount,
        int chunkSize,
        int chunkCount,
        ParallelOptions parallelOptions,
        ColumnZoneMapData?[] zoneMapData)
    {
        var partialSums = new long[chunkCount];
        var partialCounts = new int[chunkCount];
        ref readonly int valuesRef = ref MemoryMarshal.GetReference(valueArray.Values);
        int* valuesPtr = (int*)Unsafe.AsPointer(ref Unsafe.AsRef(in valuesRef));

        Parallel.For(0, chunkCount, parallelOptions, chunkIndex =>
        {
            // Zone map optimization: skip chunks that cannot contain matches
            if (CanSkipChunkViaZoneMap(predicates, zoneMapData, chunkIndex))
            {
                partialSums[chunkIndex] = 0;
                partialCounts[chunkIndex] = 0;
                return;
            }

            var startRow = chunkIndex * chunkSize;
            var endRow = Math.Min(startRow + chunkSize, rowCount);
            long localSum = 0;
            int localCount = 0;

            for (int i = startRow; i < endRow; i++)
            {
                if (!valueArray.IsNull(i) && EvaluateAllPredicates(predicates, predicateColumns, i))
                {
                    localSum += valuesPtr[i];
                    localCount++;
                }
            }

            partialSums[chunkIndex] = localSum;
            partialCounts[chunkIndex] = localCount;
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

    private static unsafe double FusedAverageInt64Parallel(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        Int64Array valueArray,
        int rowCount,
        int chunkSize,
        int chunkCount,
        ParallelOptions parallelOptions,
        ColumnZoneMapData?[] zoneMapData)
    {
        var partialSums = new long[chunkCount];
        var partialCounts = new int[chunkCount];
        ref readonly long valuesRef = ref MemoryMarshal.GetReference(valueArray.Values);
        long* valuesPtr = (long*)Unsafe.AsPointer(ref Unsafe.AsRef(in valuesRef));

        Parallel.For(0, chunkCount, parallelOptions, chunkIndex =>
        {
            // Zone map optimization: skip chunks that cannot contain matches
            if (CanSkipChunkViaZoneMap(predicates, zoneMapData, chunkIndex))
            {
                partialSums[chunkIndex] = 0;
                partialCounts[chunkIndex] = 0;
                return;
            }

            var startRow = chunkIndex * chunkSize;
            var endRow = Math.Min(startRow + chunkSize, rowCount);
            long localSum = 0;
            int localCount = 0;

            for (int i = startRow; i < endRow; i++)
            {
                if (!valueArray.IsNull(i) && EvaluateAllPredicates(predicates, predicateColumns, i))
                {
                    localSum += valuesPtr[i];
                    localCount++;
                }
            }

            partialSums[chunkIndex] = localSum;
            partialCounts[chunkIndex] = localCount;
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

    private static unsafe double FusedAverageDoubleParallel(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        DoubleArray valueArray,
        int rowCount,
        int chunkSize,
        int chunkCount,
        ParallelOptions parallelOptions,
        ColumnZoneMapData?[] zoneMapData)
    {
        var partialSums = new double[chunkCount];
        var partialCounts = new int[chunkCount];
        ref readonly double valuesRef = ref MemoryMarshal.GetReference(valueArray.Values);
        double* valuesPtr = (double*)Unsafe.AsPointer(ref Unsafe.AsRef(in valuesRef));

        Parallel.For(0, chunkCount, parallelOptions, chunkIndex =>
        {
            // Zone map optimization: skip chunks that cannot contain matches
            if (CanSkipChunkViaZoneMap(predicates, zoneMapData, chunkIndex))
            {
                partialSums[chunkIndex] = 0;
                partialCounts[chunkIndex] = 0;
                return;
            }

            var startRow = chunkIndex * chunkSize;
            var endRow = Math.Min(startRow + chunkSize, rowCount);
            double localSum = 0;
            int localCount = 0;

            for (int i = startRow; i < endRow; i++)
            {
                if (!valueArray.IsNull(i) && EvaluateAllPredicates(predicates, predicateColumns, i))
                {
                    localSum += valuesPtr[i];
                    localCount++;
                }
            }

            partialSums[chunkIndex] = localSum;
            partialCounts[chunkIndex] = localCount;
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

    private static decimal FusedAverageDecimalParallel(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        Decimal128Array valueArray,
        int rowCount,
        int chunkSize,
        int chunkCount,
        ParallelOptions parallelOptions,
        ColumnZoneMapData?[] zoneMapData)
    {
        var partialSums = new decimal[chunkCount];
        var partialCounts = new int[chunkCount];

        Parallel.For(0, chunkCount, parallelOptions, chunkIndex =>
        {
            // Zone map optimization: skip chunks that cannot contain matches
            if (CanSkipChunkViaZoneMap(predicates, zoneMapData, chunkIndex))
            {
                partialSums[chunkIndex] = 0;
                partialCounts[chunkIndex] = 0;
                return;
            }

            var startRow = chunkIndex * chunkSize;
            var endRow = Math.Min(startRow + chunkSize, rowCount);
            decimal localSum = 0;
            int localCount = 0;

            for (int i = startRow; i < endRow; i++)
            {
                if (!valueArray.IsNull(i) && EvaluateAllPredicates(predicates, predicateColumns, i))
                {
                    localSum += valueArray.GetValue(i)!.Value;
                    localCount++;
                }
            }

            partialSums[chunkIndex] = localSum;
            partialCounts[chunkIndex] = localCount;
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

    #endregion

    #region Fused Min

    private static object FusedMinSequential(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        IArrowArray aggregateColumn,
        int rowCount,
        Type resultType)
    {
        return aggregateColumn switch
        {
            Int32Array int32Array => ConvertResult(FusedMinInt32Sequential(predicates, predicateColumns, int32Array, rowCount), resultType),
            Int64Array int64Array => ConvertResult(FusedMinInt64Sequential(predicates, predicateColumns, int64Array, rowCount), resultType),
            DoubleArray doubleArray => ConvertResult(FusedMinDoubleSequential(predicates, predicateColumns, doubleArray, rowCount), resultType),
            Decimal128Array decimalArray => ConvertResult(FusedMinDecimalSequential(predicates, predicateColumns, decimalArray, rowCount), resultType),
            _ => throw new NotSupportedException($"Fused Min not supported for {aggregateColumn.GetType().Name}")
        };
    }

    private static int FusedMinInt32Sequential(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        Int32Array valueArray,
        int rowCount)
    {
        int min = int.MaxValue;
        bool hasValue = false;
        var values = valueArray.Values;

        for (int i = 0; i < rowCount; i++)
        {
            if (!valueArray.IsNull(i) && EvaluateAllPredicates(predicates, predicateColumns, i))
            {
                var value = values[i];
                if (!hasValue || value < min)
                {
                    min = value;
                    hasValue = true;
                }
            }
        }

        if (!hasValue)
            throw new InvalidOperationException("Sequence contains no elements.");
        return min;
    }

    private static long FusedMinInt64Sequential(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        Int64Array valueArray,
        int rowCount)
    {
        long min = long.MaxValue;
        bool hasValue = false;
        var values = valueArray.Values;

        for (int i = 0; i < rowCount; i++)
        {
            if (!valueArray.IsNull(i) && EvaluateAllPredicates(predicates, predicateColumns, i))
            {
                var value = values[i];
                if (!hasValue || value < min)
                {
                    min = value;
                    hasValue = true;
                }
            }
        }

        if (!hasValue)
            throw new InvalidOperationException("Sequence contains no elements.");
        return min;
    }

    private static double FusedMinDoubleSequential(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        DoubleArray valueArray,
        int rowCount)
    {
        double min = double.MaxValue;
        bool hasValue = false;
        var values = valueArray.Values;

        for (int i = 0; i < rowCount; i++)
        {
            if (!valueArray.IsNull(i) && EvaluateAllPredicates(predicates, predicateColumns, i))
            {
                var value = values[i];
                if (!hasValue || value < min)
                {
                    min = value;
                    hasValue = true;
                }
            }
        }

        if (!hasValue)
            throw new InvalidOperationException("Sequence contains no elements.");
        return min;
    }

    private static decimal FusedMinDecimalSequential(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        Decimal128Array valueArray,
        int rowCount)
    {
        decimal min = decimal.MaxValue;
        bool hasValue = false;

        for (int i = 0; i < rowCount; i++)
        {
            if (!valueArray.IsNull(i) && EvaluateAllPredicates(predicates, predicateColumns, i))
            {
                var value = valueArray.GetValue(i)!.Value;
                if (!hasValue || value < min)
                {
                    min = value;
                    hasValue = true;
                }
            }
        }

        if (!hasValue)
            throw new InvalidOperationException("Sequence contains no elements.");
        return min;
    }

    private static object FusedMinParallel(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        IArrowArray aggregateColumn,
        int rowCount,
        int chunkSize,
        int chunkCount,
        ParallelOptions parallelOptions,
        Type resultType,
        ColumnZoneMapData?[] zoneMapData)
    {
        return aggregateColumn switch
        {
            Int32Array int32Array => ConvertResult(FusedMinInt32Parallel(predicates, predicateColumns, int32Array, rowCount, chunkSize, chunkCount, parallelOptions, zoneMapData), resultType),
            Int64Array int64Array => ConvertResult(FusedMinInt64Parallel(predicates, predicateColumns, int64Array, rowCount, chunkSize, chunkCount, parallelOptions, zoneMapData), resultType),
            DoubleArray doubleArray => ConvertResult(FusedMinDoubleParallel(predicates, predicateColumns, doubleArray, rowCount, chunkSize, chunkCount, parallelOptions, zoneMapData), resultType),
            Decimal128Array decimalArray => ConvertResult(FusedMinDecimalParallel(predicates, predicateColumns, decimalArray, rowCount, chunkSize, chunkCount, parallelOptions, zoneMapData), resultType),
            _ => throw new NotSupportedException($"Fused parallel Min not supported for {aggregateColumn.GetType().Name}")
        };
    }

    private static unsafe int FusedMinInt32Parallel(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        Int32Array valueArray,
        int rowCount,
        int chunkSize,
        int chunkCount,
        ParallelOptions parallelOptions,
        ColumnZoneMapData?[] zoneMapData)
    {
        var partialMins = new int[chunkCount];
        var hasValues = new bool[chunkCount];
        ref readonly int valuesRef = ref MemoryMarshal.GetReference(valueArray.Values);
        int* valuesPtr = (int*)Unsafe.AsPointer(ref Unsafe.AsRef(in valuesRef));

        for (int i = 0; i < chunkCount; i++) partialMins[i] = int.MaxValue;

        Parallel.For(0, chunkCount, parallelOptions, chunkIndex =>
        {
            // Zone map optimization: skip chunks that cannot contain matches
            if (CanSkipChunkViaZoneMap(predicates, zoneMapData, chunkIndex))
            {
                // Leave as MaxValue with hasValues[chunkIndex] = false (default)
                return;
            }

            var startRow = chunkIndex * chunkSize;
            var endRow = Math.Min(startRow + chunkSize, rowCount);
            int localMin = int.MaxValue;
            bool foundValue = false;

            for (int i = startRow; i < endRow; i++)
            {
                if (!valueArray.IsNull(i) && EvaluateAllPredicates(predicates, predicateColumns, i))
                {
                    var value = valuesPtr[i];
                    if (!foundValue || value < localMin)
                    {
                        localMin = value;
                        foundValue = true;
                    }
                }
            }

            partialMins[chunkIndex] = localMin;
            hasValues[chunkIndex] = foundValue;
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

        if (!hasAnyValue)
            throw new InvalidOperationException("Sequence contains no elements.");
        return globalMin;
    }

    private static unsafe long FusedMinInt64Parallel(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        Int64Array valueArray,
        int rowCount,
        int chunkSize,
        int chunkCount,
        ParallelOptions parallelOptions,
        ColumnZoneMapData?[] zoneMapData)
    {
        var partialMins = new long[chunkCount];
        var hasValues = new bool[chunkCount];
        ref readonly long valuesRef = ref MemoryMarshal.GetReference(valueArray.Values);
        long* valuesPtr = (long*)Unsafe.AsPointer(ref Unsafe.AsRef(in valuesRef));

        for (int i = 0; i < chunkCount; i++) partialMins[i] = long.MaxValue;

        Parallel.For(0, chunkCount, parallelOptions, chunkIndex =>
        {
            // Zone map optimization: skip chunks that cannot contain matches
            if (CanSkipChunkViaZoneMap(predicates, zoneMapData, chunkIndex))
            {
                return;
            }

            var startRow = chunkIndex * chunkSize;
            var endRow = Math.Min(startRow + chunkSize, rowCount);
            long localMin = long.MaxValue;
            bool foundValue = false;

            for (int i = startRow; i < endRow; i++)
            {
                if (!valueArray.IsNull(i) && EvaluateAllPredicates(predicates, predicateColumns, i))
                {
                    var value = valuesPtr[i];
                    if (!foundValue || value < localMin)
                    {
                        localMin = value;
                        foundValue = true;
                    }
                }
            }

            partialMins[chunkIndex] = localMin;
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

        if (!hasAnyValue)
            throw new InvalidOperationException("Sequence contains no elements.");
        return globalMin;
    }

    private static unsafe double FusedMinDoubleParallel(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        DoubleArray valueArray,
        int rowCount,
        int chunkSize,
        int chunkCount,
        ParallelOptions parallelOptions,
        ColumnZoneMapData?[] zoneMapData)
    {
        var partialMins = new double[chunkCount];
        var hasValues = new bool[chunkCount];
        ref readonly double valuesRef = ref MemoryMarshal.GetReference(valueArray.Values);
        double* valuesPtr = (double*)Unsafe.AsPointer(ref Unsafe.AsRef(in valuesRef));

        for (int i = 0; i < chunkCount; i++) partialMins[i] = double.MaxValue;

        Parallel.For(0, chunkCount, parallelOptions, chunkIndex =>
        {
            // Zone map optimization: skip chunks that cannot contain matches
            if (CanSkipChunkViaZoneMap(predicates, zoneMapData, chunkIndex))
            {
                return;
            }

            var startRow = chunkIndex * chunkSize;
            var endRow = Math.Min(startRow + chunkSize, rowCount);
            double localMin = double.MaxValue;
            bool foundValue = false;

            for (int i = startRow; i < endRow; i++)
            {
                if (!valueArray.IsNull(i) && EvaluateAllPredicates(predicates, predicateColumns, i))
                {
                    var value = valuesPtr[i];
                    if (!foundValue || value < localMin)
                    {
                        localMin = value;
                        foundValue = true;
                    }
                }
            }

            partialMins[chunkIndex] = localMin;
            hasValues[chunkIndex] = foundValue;
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

        if (!hasAnyValue)
            throw new InvalidOperationException("Sequence contains no elements.");
        return globalMin;
    }

    private static decimal FusedMinDecimalParallel(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        Decimal128Array valueArray,
        int rowCount,
        int chunkSize,
        int chunkCount,
        ParallelOptions parallelOptions,
        ColumnZoneMapData?[] zoneMapData)
    {
        var partialMins = new decimal[chunkCount];
        var hasValues = new bool[chunkCount];

        for (int i = 0; i < chunkCount; i++) partialMins[i] = decimal.MaxValue;

        Parallel.For(0, chunkCount, parallelOptions, chunkIndex =>
        {
            // Zone map optimization: skip chunks that cannot contain matches
            if (CanSkipChunkViaZoneMap(predicates, zoneMapData, chunkIndex))
            {
                return;
            }

            var startRow = chunkIndex * chunkSize;
            var endRow = Math.Min(startRow + chunkSize, rowCount);
            decimal localMin = decimal.MaxValue;
            bool foundValue = false;

            for (int i = startRow; i < endRow; i++)
            {
                if (!valueArray.IsNull(i) && EvaluateAllPredicates(predicates, predicateColumns, i))
                {
                    var value = valueArray.GetValue(i)!.Value;
                    if (!foundValue || value < localMin)
                    {
                        localMin = value;
                        foundValue = true;
                    }
                }
            }

            partialMins[chunkIndex] = localMin;
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

        if (!hasAnyValue)
            throw new InvalidOperationException("Sequence contains no elements.");
        return globalMin;
    }

    #endregion

    #region Fused Max

    private static object FusedMaxSequential(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        IArrowArray aggregateColumn,
        int rowCount,
        Type resultType)
    {
        return aggregateColumn switch
        {
            Int32Array int32Array => ConvertResult(FusedMaxInt32Sequential(predicates, predicateColumns, int32Array, rowCount), resultType),
            Int64Array int64Array => ConvertResult(FusedMaxInt64Sequential(predicates, predicateColumns, int64Array, rowCount), resultType),
            DoubleArray doubleArray => ConvertResult(FusedMaxDoubleSequential(predicates, predicateColumns, doubleArray, rowCount), resultType),
            Decimal128Array decimalArray => ConvertResult(FusedMaxDecimalSequential(predicates, predicateColumns, decimalArray, rowCount), resultType),
            _ => throw new NotSupportedException($"Fused Max not supported for {aggregateColumn.GetType().Name}")
        };
    }

    private static int FusedMaxInt32Sequential(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        Int32Array valueArray,
        int rowCount)
    {
        int max = int.MinValue;
        bool hasValue = false;
        var values = valueArray.Values;

        for (int i = 0; i < rowCount; i++)
        {
            if (!valueArray.IsNull(i) && EvaluateAllPredicates(predicates, predicateColumns, i))
            {
                var value = values[i];
                if (!hasValue || value > max)
                {
                    max = value;
                    hasValue = true;
                }
            }
        }

        if (!hasValue)
            throw new InvalidOperationException("Sequence contains no elements.");
        return max;
    }

    private static long FusedMaxInt64Sequential(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        Int64Array valueArray,
        int rowCount)
    {
        long max = long.MinValue;
        bool hasValue = false;
        var values = valueArray.Values;

        for (int i = 0; i < rowCount; i++)
        {
            if (!valueArray.IsNull(i) && EvaluateAllPredicates(predicates, predicateColumns, i))
            {
                var value = values[i];
                if (!hasValue || value > max)
                {
                    max = value;
                    hasValue = true;
                }
            }
        }

        if (!hasValue)
            throw new InvalidOperationException("Sequence contains no elements.");
        return max;
    }

    private static double FusedMaxDoubleSequential(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        DoubleArray valueArray,
        int rowCount)
    {
        double max = double.MinValue;
        bool hasValue = false;
        var values = valueArray.Values;

        for (int i = 0; i < rowCount; i++)
        {
            if (!valueArray.IsNull(i) && EvaluateAllPredicates(predicates, predicateColumns, i))
            {
                var value = values[i];
                if (!hasValue || value > max)
                {
                    max = value;
                    hasValue = true;
                }
            }
        }

        if (!hasValue)
            throw new InvalidOperationException("Sequence contains no elements.");
        return max;
    }

    private static decimal FusedMaxDecimalSequential(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        Decimal128Array valueArray,
        int rowCount)
    {
        decimal max = decimal.MinValue;
        bool hasValue = false;

        for (int i = 0; i < rowCount; i++)
        {
            if (!valueArray.IsNull(i) && EvaluateAllPredicates(predicates, predicateColumns, i))
            {
                var value = valueArray.GetValue(i)!.Value;
                if (!hasValue || value > max)
                {
                    max = value;
                    hasValue = true;
                }
            }
        }

        if (!hasValue)
            throw new InvalidOperationException("Sequence contains no elements.");
        return max;
    }

    private static object FusedMaxParallel(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        IArrowArray aggregateColumn,
        int rowCount,
        int chunkSize,
        int chunkCount,
        ParallelOptions parallelOptions,
        Type resultType,
        ColumnZoneMapData?[] zoneMapData)
    {
        return aggregateColumn switch
        {
            Int32Array int32Array => ConvertResult(FusedMaxInt32Parallel(predicates, predicateColumns, int32Array, rowCount, chunkSize, chunkCount, parallelOptions, zoneMapData), resultType),
            Int64Array int64Array => ConvertResult(FusedMaxInt64Parallel(predicates, predicateColumns, int64Array, rowCount, chunkSize, chunkCount, parallelOptions, zoneMapData), resultType),
            DoubleArray doubleArray => ConvertResult(FusedMaxDoubleParallel(predicates, predicateColumns, doubleArray, rowCount, chunkSize, chunkCount, parallelOptions, zoneMapData), resultType),
            Decimal128Array decimalArray => ConvertResult(FusedMaxDecimalParallel(predicates, predicateColumns, decimalArray, rowCount, chunkSize, chunkCount, parallelOptions, zoneMapData), resultType),
            _ => throw new NotSupportedException($"Fused parallel Max not supported for {aggregateColumn.GetType().Name}")
        };
    }

    private static unsafe int FusedMaxInt32Parallel(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        Int32Array valueArray,
        int rowCount,
        int chunkSize,
        int chunkCount,
        ParallelOptions parallelOptions,
        ColumnZoneMapData?[] zoneMapData)
    {
        var partialMaxs = new int[chunkCount];
        var hasValues = new bool[chunkCount];
        ref readonly int valuesRef = ref MemoryMarshal.GetReference(valueArray.Values);
        int* valuesPtr = (int*)Unsafe.AsPointer(ref Unsafe.AsRef(in valuesRef));

        for (int i = 0; i < chunkCount; i++) partialMaxs[i] = int.MinValue;

        Parallel.For(0, chunkCount, parallelOptions, chunkIndex =>
        {
            // Zone map optimization: skip chunks that cannot contain matches
            if (CanSkipChunkViaZoneMap(predicates, zoneMapData, chunkIndex))
            {
                return;
            }

            var startRow = chunkIndex * chunkSize;
            var endRow = Math.Min(startRow + chunkSize, rowCount);
            int localMax = int.MinValue;
            bool foundValue = false;

            for (int i = startRow; i < endRow; i++)
            {
                if (!valueArray.IsNull(i) && EvaluateAllPredicates(predicates, predicateColumns, i))
                {
                    var value = valuesPtr[i];
                    if (!foundValue || value > localMax)
                    {
                        localMax = value;
                        foundValue = true;
                    }
                }
            }

            partialMaxs[chunkIndex] = localMax;
            hasValues[chunkIndex] = foundValue;
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

        if (!hasAnyValue)
            throw new InvalidOperationException("Sequence contains no elements.");
        return globalMax;
    }

    private static unsafe long FusedMaxInt64Parallel(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        Int64Array valueArray,
        int rowCount,
        int chunkSize,
        int chunkCount,
        ParallelOptions parallelOptions,
        ColumnZoneMapData?[] zoneMapData)
    {
        var partialMaxs = new long[chunkCount];
        var hasValues = new bool[chunkCount];
        ref readonly long valuesRef = ref MemoryMarshal.GetReference(valueArray.Values);
        long* valuesPtr = (long*)Unsafe.AsPointer(ref Unsafe.AsRef(in valuesRef));

        for (int i = 0; i < chunkCount; i++) partialMaxs[i] = long.MinValue;

        Parallel.For(0, chunkCount, parallelOptions, chunkIndex =>
        {
            // Zone map optimization: skip chunks that cannot contain matches
            if (CanSkipChunkViaZoneMap(predicates, zoneMapData, chunkIndex))
            {
                return;
            }

            var startRow = chunkIndex * chunkSize;
            var endRow = Math.Min(startRow + chunkSize, rowCount);
            long localMax = long.MinValue;
            bool foundValue = false;

            for (int i = startRow; i < endRow; i++)
            {
                if (!valueArray.IsNull(i) && EvaluateAllPredicates(predicates, predicateColumns, i))
                {
                    var value = valuesPtr[i];
                    if (!foundValue || value > localMax)
                    {
                        localMax = value;
                        foundValue = true;
                    }
                }
            }

            partialMaxs[chunkIndex] = localMax;
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

        if (!hasAnyValue)
            throw new InvalidOperationException("Sequence contains no elements.");
        return globalMax;
    }

    private static unsafe double FusedMaxDoubleParallel(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        DoubleArray valueArray,
        int rowCount,
        int chunkSize,
        int chunkCount,
        ParallelOptions parallelOptions,
        ColumnZoneMapData?[] zoneMapData)
    {
        var partialMaxs = new double[chunkCount];
        var hasValues = new bool[chunkCount];
        ref readonly double valuesRef = ref MemoryMarshal.GetReference(valueArray.Values);
        double* valuesPtr = (double*)Unsafe.AsPointer(ref Unsafe.AsRef(in valuesRef));

        for (int i = 0; i < chunkCount; i++) partialMaxs[i] = double.MinValue;

        Parallel.For(0, chunkCount, parallelOptions, chunkIndex =>
        {
            // Zone map optimization: skip chunks that cannot contain matches
            if (CanSkipChunkViaZoneMap(predicates, zoneMapData, chunkIndex))
            {
                return;
            }

            var startRow = chunkIndex * chunkSize;
            var endRow = Math.Min(startRow + chunkSize, rowCount);
            double localMax = double.MinValue;
            bool foundValue = false;

            for (int i = startRow; i < endRow; i++)
            {
                if (!valueArray.IsNull(i) && EvaluateAllPredicates(predicates, predicateColumns, i))
                {
                    var value = valuesPtr[i];
                    if (!foundValue || value > localMax)
                    {
                        localMax = value;
                        foundValue = true;
                    }
                }
            }

            partialMaxs[chunkIndex] = localMax;
            hasValues[chunkIndex] = foundValue;
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

        if (!hasAnyValue)
            throw new InvalidOperationException("Sequence contains no elements.");
        return globalMax;
    }

    private static decimal FusedMaxDecimalParallel(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        Decimal128Array valueArray,
        int rowCount,
        int chunkSize,
        int chunkCount,
        ParallelOptions parallelOptions,
        ColumnZoneMapData?[] zoneMapData)
    {
        var partialMaxs = new decimal[chunkCount];
        var hasValues = new bool[chunkCount];

        for (int i = 0; i < chunkCount; i++) partialMaxs[i] = decimal.MinValue;

        Parallel.For(0, chunkCount, parallelOptions, chunkIndex =>
        {
            // Zone map optimization: skip chunks that cannot contain matches
            if (CanSkipChunkViaZoneMap(predicates, zoneMapData, chunkIndex))
            {
                return;
            }

            var startRow = chunkIndex * chunkSize;
            var endRow = Math.Min(startRow + chunkSize, rowCount);
            decimal localMax = decimal.MinValue;
            bool foundValue = false;

            for (int i = startRow; i < endRow; i++)
            {
                if (!valueArray.IsNull(i) && EvaluateAllPredicates(predicates, predicateColumns, i))
                {
                    var value = valueArray.GetValue(i)!.Value;
                    if (!foundValue || value > localMax)
                    {
                        localMax = value;
                        foundValue = true;
                    }
                }
            }

            partialMaxs[chunkIndex] = localMax;
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

        if (!hasAnyValue)
            throw new InvalidOperationException("Sequence contains no elements.");
        return globalMax;
    }

    #endregion

    #region Helper Methods

    /// <summary>
    /// Evaluates all predicates for a single row. Returns true if all pass.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool EvaluateAllPredicates(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        int rowIndex)
    {
        for (int p = 0; p < predicates.Count; p++)
        {
            if (!predicates[p].EvaluateSingleRow(predicateColumns[p], rowIndex))
            {
                return false;
            }
        }
        return true;
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
