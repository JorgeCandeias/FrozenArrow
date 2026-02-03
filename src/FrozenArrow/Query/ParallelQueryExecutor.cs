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
    /// </summary>
    /// <param name="batch">The Arrow record batch to evaluate.</param>
    /// <param name="selection">The selection bitmap to update.</param>
    /// <param name="predicates">The predicates to evaluate.</param>
    /// <param name="options">Parallel execution options.</param>
    public static unsafe void EvaluatePredicatesParallel(
        RecordBatch batch,
        ref SelectionBitmap selection,
        IReadOnlyList<ColumnPredicate> predicates,
        ParallelQueryOptions? options = null)
    {
        options ??= ParallelQueryOptions.Default;
        var rowCount = batch.Length;

        // Fall back to sequential for small datasets or when disabled
        if (!options.EnableParallelExecution || 
            rowCount < options.ParallelThreshold || 
            predicates.Count == 0)
        {
            EvaluatePredicatesSequential(batch, ref selection, predicates);
            return;
        }

        var chunkSize = options.ChunkSize;
        var chunkCount = (rowCount + chunkSize - 1) / chunkSize;

        // For single chunk, use sequential
        if (chunkCount == 1)
        {
            EvaluatePredicatesSequential(batch, ref selection, predicates);
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

        // Get a pointer to the selection bitmap.
        // This is safe because:
        // 1. The bitmap outlives the parallel operation (it's created in ExecutePlan with 'using')
        // 2. We write to non-overlapping bit ranges from different threads
        // 3. The struct remains at the same address during the parallel operation
        SelectionBitmap* selectionPtr = (SelectionBitmap*)Unsafe.AsPointer(ref selection);

        // Process chunks in parallel
        Parallel.For(0, chunkCount, parallelOptions, chunkIndex =>
        {
            var startRow = chunkIndex * chunkSize;
            var endRow = Math.Min(startRow + chunkSize, rowCount);

            // Get a reference to the selection bitmap from the pointer
            ref var sel = ref Unsafe.AsRef<SelectionBitmap>(selectionPtr);

            // Apply each predicate to this chunk
            for (int predIdx = 0; predIdx < predicates.Count; predIdx++)
            {
                var predicate = predicates[predIdx];
                var column = columns[predIdx];
                
                predicate.EvaluateRange(column, ref sel, startRow, endRow);
            }
        });
    }

    /// <summary>
    /// Evaluates predicates sequentially (original behavior).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void EvaluatePredicatesSequential(
        RecordBatch batch,
        ref SelectionBitmap selection,
        IReadOnlyList<ColumnPredicate> predicates)
    {
        foreach (var predicate in predicates)
        {
            predicate.Evaluate(batch, ref selection);
        }
    }
}
