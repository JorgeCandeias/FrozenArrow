using System.Buffers;
using Apache.Arrow;

namespace FrozenArrow.Query;

/// <summary>
/// Collects matching row indices for sparse predicates without materializing a full bitmap.
/// This is more efficient than SelectionBitmap when selectivity is very low (&lt;5%).
/// </summary>
/// <remarks>
/// For highly selective queries (e.g., 1% of rows match), a full bitmap wastes memory:
/// - Bitmap: 125 KB for 1M rows (always allocated)
/// - Sparse list: ~40 KB for 10K matches (2.5MB for array, grows as needed)
/// 
/// Memory comparison (1M rows):
/// - 1% selectivity: 10K matches ? List~40KB vs Bitmap 125KB (3× savings)
/// - 5% selectivity: 50K matches ? List~200KB vs Bitmap 125KB (break-even)
/// - 10% selectivity: 100K matches ? List~400KB vs Bitmap 125KB (1.6× worse, use bitmap)
/// </remarks>
internal static class SparseIndexCollector
{
    /// <summary>
    /// Evaluates predicates and collects matching indices into a list.
    /// Only rows that pass ALL predicates are added.
    /// </summary>
    /// <param name="batch">The record batch to evaluate.</param>
    /// <param name="predicates">The predicates to evaluate.</param>
    /// <param name="zoneMap">Optional zone map for skip-scanning.</param>
    /// <param name="options">Parallel execution options.</param>
    /// <param name="maxRowToEvaluate">Maximum row index to evaluate (exclusive). If null, evaluates all rows.</param>
    /// <param name="minRowToEvaluate">Minimum row index to evaluate (inclusive). Default is 0.</param>
    /// <param name="maxIndicesToCollect">Maximum number of indices to collect. Stops early when reached. Null means collect all.</param>
    /// <returns>List of row indices that match all predicates.</returns>
    public static List<int> CollectMatchingIndices(
        RecordBatch batch,
        IReadOnlyList<ColumnPredicate> predicates,
        ZoneMap? zoneMap = null,
        ParallelQueryOptions? options = null,
        int? maxRowToEvaluate = null,
        int minRowToEvaluate = 0,
        int? maxIndicesToCollect = null)
    {
        options ??= ParallelQueryOptions.Default;
        var rowCount = maxRowToEvaluate ?? batch.Length;
        
        // Reorder predicates by estimated selectivity (most selective first)
        predicates = PredicateReorderer.ReorderBySelectivity(predicates, zoneMap, rowCount);
        
        // Pre-fetch columns
        var columns = new IArrowArray[predicates.Count];
        for (int i = 0; i < predicates.Count; i++)
        {
            columns[i] = batch.Column(predicates[i].ColumnIndex);
        }
        
        // Pre-fetch zone map data
        var zoneMapData = new ColumnZoneMapData?[predicates.Count];
        if (zoneMap != null)
        {
            for (int i = 0; i < predicates.Count; i++)
            {
                zoneMap.TryGetColumnZoneMap(predicates[i].ColumnName, out zoneMapData[i]);
            }
        }
        
        // Estimate capacity: assume 5% selectivity (upper bound for sparse collection)
        var estimatedCapacity = maxIndicesToCollect.HasValue 
            ? Math.Min(maxIndicesToCollect.Value, Math.Max(1000, rowCount / 20))
            : Math.Max(1000, rowCount / 20);
        var result = new List<int>(estimatedCapacity);
        
        // Sequential collection for small datasets
        if (rowCount < options.ParallelThreshold || !options.EnableParallelExecution)
        {
            CollectSequential(predicates, columns, zoneMapData, zoneMap, minRowToEvaluate, rowCount, result, maxIndicesToCollect);
            return result;
        }
        
        // Parallel collection for large datasets
        // Note: Early termination is less effective in parallel mode, but still beneficial
        return CollectParallel(predicates, columns, zoneMapData, zoneMap, minRowToEvaluate, rowCount, options, maxIndicesToCollect);
    }
    
    /// <summary>
    /// Sequential index collection - evaluates predicates row-by-row.
    /// </summary>
    private static void CollectSequential(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] columns,
        ColumnZoneMapData?[] zoneMapData,
        ZoneMap? zoneMap,
        int startRow,
        int endRow,
        List<int> result,
        int? maxIndicesToCollect = null)
    {
        var chunkSize = 16_384;
        var chunkCount = (endRow - startRow + chunkSize - 1) / chunkSize;
        
        for (int chunkIndex = 0; chunkIndex < chunkCount; chunkIndex++)
        {
            // Early termination: stop once we've collected enough indices
            if (maxIndicesToCollect.HasValue && result.Count >= maxIndicesToCollect.Value)
            {
                break;
            }
            
            int chunkStart = startRow + chunkIndex * chunkSize;
            int chunkEnd = Math.Min(chunkStart + chunkSize, endRow);
            
            // Zone map skip: check if ANY predicate excludes this chunk
            if (CanSkipChunk(predicates, zoneMapData, chunkIndex))
            {
                continue; // Skip entire chunk
            }
            
            // Evaluate rows in this chunk
            for (int row = chunkStart; row < chunkEnd; row++)
            {
                if (EvaluateAllPredicates(predicates, columns, row))
                {
                    result.Add(row);
                    
                    // Early termination: stop immediately once we have enough indices
                    if (maxIndicesToCollect.HasValue && result.Count >= maxIndicesToCollect.Value)
                    {
                        return; // Exit the method early
                    }
                }
            }
        }
    }
    
    /// <summary>
    /// Parallel index collection - each thread collects indices independently.
    /// </summary>
    private static List<int> CollectParallel(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] columns,
        ColumnZoneMapData?[] zoneMapData,
        ZoneMap? zoneMap,
        int startRow,
        int endRow,
        ParallelQueryOptions options,
        int? maxIndicesToCollect = null)
    {
        var chunkSize = options.ChunkSize;
        var rowCount = endRow - startRow;
        var chunkCount = (rowCount + chunkSize - 1) / chunkSize;
        
        var parallelOptions = new ParallelOptions();
        if (options.MaxDegreeOfParallelism > 0)
        {
            parallelOptions.MaxDegreeOfParallelism = options.MaxDegreeOfParallelism;
        }
        
        // Shared counter for early termination (when maxIndicesToCollect is specified)
        int collectedCount = 0;
        
        // Each thread collects into its own list to avoid contention
        var threadLocalResults = new System.Collections.Concurrent.ConcurrentBag<List<int>>();
        
        Parallel.For(0, chunkCount, parallelOptions, () => new List<int>(1000), (chunkIndex, state, threadList) =>
        {
            // Early termination check (check periodically, not per-row for performance)
            if (maxIndicesToCollect.HasValue && 
                Interlocked.CompareExchange(ref collectedCount, 0, 0) >= maxIndicesToCollect.Value)
            {
                state.Stop(); // Signal other threads to stop
                return threadList;
            }
            
            int chunkStart = startRow + chunkIndex * chunkSize;
            int chunkEnd = Math.Min(chunkStart + chunkSize, endRow);
            
            // Zone map skip
            if (CanSkipChunk(predicates, zoneMapData, chunkIndex))
            {
                return threadList;
            }
            
            // Evaluate rows in this chunk
            for (int row = chunkStart; row < chunkEnd; row++)
            {
                if (EvaluateAllPredicates(predicates, columns, row))
                {
                    threadList.Add(row);
                    
                    // Update shared counter and check if we've collected enough
                    if (maxIndicesToCollect.HasValue)
                    {
                        int currentCount = Interlocked.Increment(ref collectedCount);
                        if (currentCount >= maxIndicesToCollect.Value)
                        {
                            state.Stop(); // Signal other threads to stop
                            return threadList;
                        }
                    }
                }
            }
            
            return threadList;
        },
        threadList =>
        {
            if (threadList.Count > 0)
            {
                threadLocalResults.Add(threadList);
            }
        });
        
        // Merge thread-local results
        var totalCapacity = 0;
        foreach (var list in threadLocalResults)
        {
            totalCapacity += list.Count;
        }
        
        // If we have a limit and exceeded it, trim during merge
        if (maxIndicesToCollect.HasValue && totalCapacity > maxIndicesToCollect.Value)
        {
            totalCapacity = maxIndicesToCollect.Value;
        }
        
        var merged = new List<int>(totalCapacity);
        foreach (var list in threadLocalResults)
        {
            merged.AddRange(list);
            
            // Stop merging if we've reached the limit
            if (maxIndicesToCollect.HasValue && merged.Count >= maxIndicesToCollect.Value)
            {
                break;
            }
        }
        
        // Sort indices for sequential access (better cache locality during enumeration)
        merged.Sort();
        
        // Trim to exact limit if we over-collected (can happen due to parallel race)
        if (maxIndicesToCollect.HasValue && merged.Count > maxIndicesToCollect.Value)
        {
            merged.RemoveRange(maxIndicesToCollect.Value, merged.Count - maxIndicesToCollect.Value);
        }
        
        return merged;
    }
    
    /// <summary>
    /// Checks if a chunk can be skipped based on zone maps.
    /// </summary>
    private static bool CanSkipChunk(
        IReadOnlyList<ColumnPredicate> predicates,
        ColumnZoneMapData?[] zoneMapData,
        int chunkIndex)
    {
        for (int i = 0; i < predicates.Count; i++)
        {
            if (!predicates[i].MayContainMatches(zoneMapData[i], chunkIndex))
            {
                return true; // This predicate excludes the chunk
            }
        }
        return false;
    }
    
    /// <summary>
    /// Evaluates all predicates for a single row.
    /// </summary>
    private static bool EvaluateAllPredicates(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] columns,
        int row)
    {
        for (int i = 0; i < predicates.Count; i++)
        {
            if (!predicates[i].EvaluateSingleRow(columns[i], row))
            {
                return false; // Short-circuit on first failure
            }
        }
        return true;
    }
}
