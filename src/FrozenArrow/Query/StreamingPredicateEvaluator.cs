using System.Runtime.CompilerServices;
using Apache.Arrow;

namespace FrozenArrow.Query;

/// <summary>
/// Provides streaming predicate evaluation for short-circuit operations.
/// Unlike bitmap-based evaluation, this evaluates predicates row-by-row and
/// can stop as soon as a condition is met (e.g., first match found for Any/First).
/// </summary>
/// <remarks>
/// This is optimized for operations that don't need to process all rows:
/// - Any() - returns true on first match
/// - First() - returns first matching element
/// - Take(n) - returns first n matching elements
/// - All() - returns false on first non-match
/// 
/// For operations that need all rows (Count, Sum, ToList), use the bitmap-based
/// approach which benefits from SIMD parallelism.
/// 
/// Zone maps are used to skip entire chunks that cannot contain matches,
/// providing O(1) skip instead of O(chunk_size) evaluation.
/// 
/// Predicates are automatically reordered by estimated selectivity for optimal
/// short-circuit behavior (most selective predicates are evaluated first).
/// </remarks>
internal static class StreamingPredicateEvaluator
{
    /// <summary>
    /// Finds the first row index that matches all predicates.
    /// Returns -1 if no match is found.
    /// </summary>
    /// <param name="batch">The record batch to search.</param>
    /// <param name="predicates">The predicates that must all be satisfied.</param>
    /// <param name="zoneMap">Optional zone map for skip-scanning optimization.</param>
    /// <param name="chunkSize">Chunk size for zone map alignment (default: 16384).</param>
    /// <returns>The index of the first matching row, or -1 if none found.</returns>
    public static int FindFirst(
        RecordBatch batch,
        IReadOnlyList<ColumnPredicate> predicates,
        ZoneMap? zoneMap = null,
        int chunkSize = 16_384)
    {
        if (predicates.Count == 0)
            return batch.Length > 0 ? 0 : -1;

        var rowCount = batch.Length;
        
        // Reorder predicates by estimated selectivity (most selective first).
        // For short-circuit evaluation, this maximizes the chance of early rejection.
        predicates = PredicateReorderer.ReorderBySelectivity(predicates, zoneMap, rowCount);
        
        // Pre-fetch columns for all predicates
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

        // Process chunk by chunk for zone map optimization
        int chunkCount = (rowCount + chunkSize - 1) / chunkSize;
        
        for (int chunkIndex = 0; chunkIndex < chunkCount; chunkIndex++)
        {
            int startRow = chunkIndex * chunkSize;
            int endRow = Math.Min(startRow + chunkSize, rowCount);

            // Zone map skip: check if ANY predicate excludes this chunk
            if (CanSkipChunk(predicates, zoneMapData, chunkIndex))
            {
                continue; // Skip entire chunk
            }

            // Evaluate rows in this chunk
            for (int row = startRow; row < endRow; row++)
            {
                if (EvaluateAllPredicates(predicates, columns, row))
                {
                    return row; // Found first match!
                }
            }
        }

        return -1; // No match found
    }

    /// <summary>
    /// Checks if any row matches all predicates (short-circuit on first match).
    /// </summary>
    public static bool Any(
        RecordBatch batch,
        IReadOnlyList<ColumnPredicate> predicates,
        ZoneMap? zoneMap = null,
        int chunkSize = 16_384)
    {
        return FindFirst(batch, predicates, zoneMap, chunkSize) >= 0;
    }

    /// <summary>
    /// Checks if ALL rows match all predicates (short-circuit on first non-match).
    /// </summary>
    public static bool All(
        RecordBatch batch,
        IReadOnlyList<ColumnPredicate> predicates,
        ZoneMap? zoneMap = null,
        int chunkSize = 16_384)
    {
        if (predicates.Count == 0)
            return true;

        var rowCount = batch.Length;
        if (rowCount == 0)
            return true;
        
        // Pre-fetch columns
        var columns = new IArrowArray[predicates.Count];
        for (int i = 0; i < predicates.Count; i++)
        {
            columns[i] = batch.Column(predicates[i].ColumnIndex);
        }

        // For All(), we need to check every row (unless we find a non-match)
        // Zone maps can help skip chunks that are GUARANTEED to match,
        // but that's the opposite of what we have (zone maps tell us chunks that CAN'T match)
        // So zone maps don't help much for All() - we need to check all rows
        
        for (int row = 0; row < rowCount; row++)
        {
            if (!EvaluateAllPredicates(predicates, columns, row))
            {
                return false; // Found non-match!
            }
        }

        return true; // All rows match
    }

    /// <summary>
    /// Finds up to 'count' row indices that match all predicates.
    /// </summary>
    public static List<int> FindFirstN(
        RecordBatch batch,
        IReadOnlyList<ColumnPredicate> predicates,
        int count,
        ZoneMap? zoneMap = null,
        int chunkSize = 16_384)
    {
        var results = new List<int>(Math.Min(count, 1000));
        
        if (count <= 0 || predicates.Count == 0)
        {
            // No predicates - return first N indices
            if (predicates.Count == 0)
            {
                for (int i = 0; i < Math.Min(count, batch.Length); i++)
                {
                    results.Add(i);
                }
            }
            return results;
        }

        var rowCount = batch.Length;
        
        // Reorder predicates by estimated selectivity (most selective first).
        // For short-circuit evaluation, this maximizes the chance of early rejection.
        predicates = PredicateReorderer.ReorderBySelectivity(predicates, zoneMap, rowCount);
        
        // Pre-fetch columns (order matches reordered predicates)
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

        int chunkCount = (rowCount + chunkSize - 1) / chunkSize;
        
        for (int chunkIndex = 0; chunkIndex < chunkCount && results.Count < count; chunkIndex++)
        {
            int startRow = chunkIndex * chunkSize;
            int endRow = Math.Min(startRow + chunkSize, rowCount);

            // Zone map skip
            if (CanSkipChunk(predicates, zoneMapData, chunkIndex))
            {
                continue;
            }

            // Evaluate rows in this chunk
            for (int row = startRow; row < endRow && results.Count < count; row++)
            {
                if (EvaluateAllPredicates(predicates, columns, row))
                {
                    results.Add(row);
                }
            }
        }

        return results;
    }

    /// <summary>
    /// Checks if a chunk can be skipped based on zone maps.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool CanSkipChunk(
        IReadOnlyList<ColumnPredicate> predicates,
        ColumnZoneMapData?[] zoneMapData,
        int chunkIndex)
    {
        // A chunk can be skipped if ANY predicate says it cannot possibly contain matches
        for (int i = 0; i < predicates.Count; i++)
        {
            if (!predicates[i].MayContainMatches(zoneMapData[i], chunkIndex))
            {
                return true;
            }
        }
        return false;
    }

    /// <summary>
    /// Evaluates all predicates for a single row.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool EvaluateAllPredicates(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] columns,
        int row)
    {
        for (int i = 0; i < predicates.Count; i++)
        {
            if (!predicates[i].EvaluateSingleRow(columns[i], row))
            {
                return false; // Short-circuit on first failing predicate
            }
        }
        return true;
    }
}
