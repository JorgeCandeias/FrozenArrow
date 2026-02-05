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
    /// <returns>List of row indices that match all predicates.</returns>
    public static List<int> CollectMatchingIndices(
        RecordBatch batch,
        IReadOnlyList<ColumnPredicate> predicates,
        ZoneMap? zoneMap = null,
        ParallelQueryOptions? options = null,
        int? maxRowToEvaluate = null,
        int minRowToEvaluate = 0)
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
        var estimatedCapacity = Math.Max(1000, rowCount / 20);
        var result = new List<int>(estimatedCapacity);
        
        // Sequential collection for small datasets
        if (rowCount < options.ParallelThreshold || !options.EnableParallelExecution)
        {
            CollectSequential(predicates, columns, zoneMapData, zoneMap, minRowToEvaluate, rowCount, result);
            return result;
        }
        
        // Parallel collection for large datasets
        return CollectParallel(predicates, columns, zoneMapData, zoneMap, minRowToEvaluate, rowCount, options);
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
        List<int> result)
    {
        var chunkSize = 16_384;
        var chunkCount = (endRow - startRow + chunkSize - 1) / chunkSize;
        
        for (int chunkIndex = 0; chunkIndex < chunkCount; chunkIndex++)
        {
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
        ParallelQueryOptions options)
    {
        var chunkSize = options.ChunkSize;
        var rowCount = endRow - startRow;
        var chunkCount = (rowCount + chunkSize - 1) / chunkSize;
        
        var parallelOptions = new ParallelOptions();
        if (options.MaxDegreeOfParallelism > 0)
        {
            parallelOptions.MaxDegreeOfParallelism = options.MaxDegreeOfParallelism;
        }
        
        // Each thread collects into its own list to avoid contention
        var threadLocalResults = new System.Collections.Concurrent.ConcurrentBag<List<int>>();
        
        Parallel.For(0, chunkCount, parallelOptions, () => new List<int>(1000), (chunkIndex, state, threadList) =>
        {
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
        
        var merged = new List<int>(totalCapacity);
        foreach (var list in threadLocalResults)
        {
            merged.AddRange(list);
        }
        
        // Sort indices for sequential access (better cache locality during enumeration)
        merged.Sort();
        
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
