using System.Runtime.CompilerServices;
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
    /// </summary>
    /// <param name="batch">The Arrow record batch to evaluate.</param>
    /// <param name="selection">The selection bitmap to update.</param>
    /// <param name="predicates">The predicates to evaluate.</param>
    /// <param name="options">Parallel execution options.</param>
    /// <param name="zoneMap">Optional zone map for skip-scanning optimization.</param>
    public static void EvaluatePredicatesParallel(
        RecordBatch batch,
        ref SelectionBitmap selection,
        IReadOnlyList<ColumnPredicate> predicates,
        ParallelQueryOptions? options = null,
        ZoneMap? zoneMap = null)
    {
        options ??= ParallelQueryOptions.Default;
        var rowCount = batch.Length;

        // Fall back to sequential for small datasets or when disabled
        if (!options.EnableParallelExecution || 
            rowCount < options.ParallelThreshold || 
            predicates.Count == 0)
        {
            EvaluatePredicatesSequential(batch, ref selection, predicates, zoneMap);
            return;
        }

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
            for (int predIdx = 0; predIdx < predicates.Count; predIdx++)
            {
                var predicate = predicates[predIdx];
                var column = columns[predIdx];
                
                predicate.EvaluateRangeWithBuffer(column, selectionBuffer, startRow, endRow);
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
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void EvaluatePredicatesSequential(
        RecordBatch batch,
        ref SelectionBitmap selection,
        IReadOnlyList<ColumnPredicate> predicates,
        ZoneMap? zoneMap = null)
    {
        foreach (var predicate in predicates)
        {
            predicate.Evaluate(batch, ref selection);
        }
    }
}
