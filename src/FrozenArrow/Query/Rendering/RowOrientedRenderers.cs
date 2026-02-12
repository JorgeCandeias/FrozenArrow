using Apache.Arrow;

namespace FrozenArrow.Query.Rendering;

/// <summary>
/// Materializes query results to List{T} (row-oriented).
/// Uses PooledBatchMaterializer for efficient object creation.
/// </summary>
/// <typeparam name="T">The element type to materialize.</typeparam>
/// <remarks>
/// <para>
/// This renderer implements the existing FrozenArrow behavior - materializing
/// filtered rows into strongly-typed objects. It uses PooledBatchMaterializer
/// for efficient allocation (ArrayPool, parallel processing, etc.).
/// </para>
/// 
/// <para>
/// Performance characteristics:
/// - Uses ArrayPool for temporary buffers (90% reduction in allocations)
/// - Pre-allocates final array with exact capacity (zero resize overhead)
/// - Parallel chunked processing for large result sets (&gt;10K items)
/// - Direct array indexing (no List&lt;T&gt; wrapper allocation)
/// </para>
/// 
/// <para>
/// When to use:
/// - You need strongly-typed .NET objects
/// - You'll iterate over results multiple times
/// - You need to mutate results (though FrozenArrow is typically read-only)
/// - You're integrating with existing .NET APIs expecting IEnumerable&lt;T&gt;
/// </para>
/// 
/// <para>
/// When NOT to use:
/// - Exporting to Arrow IPC format (use ArrowIpcRenderer instead)
/// - Streaming to JSON/CSV (use streaming renderers instead)
/// - Large result sets you'll iterate once (use enumerable renderers)
/// </para>
/// </remarks>
/// <remarks>
/// Creates a new ListRenderer.
/// </remarks>
/// <param name="createItem">Function to create an item from a RecordBatch and row index.</param>
/// <param name="parallelOptions">Optional parallel execution options.</param>
internal sealed class ListRenderer<T>(
    Func<RecordBatch, int, T> createItem,
    ParallelQueryOptions? parallelOptions = null) : IResultRenderer<List<T>>
{
    private readonly Func<RecordBatch, int, T> _createItem = createItem ?? throw new ArgumentNullException(nameof(createItem));

    /// <summary>
    /// Renders the query result to a List{T}.
    /// </summary>
    /// <param name="queryResult">The query result to materialize.</param>
    /// <returns>A List containing materialized objects for each selected row.</returns>
    /// <remarks>
    /// This uses PooledBatchMaterializer internally, which provides:
    /// - Pooled temporary buffers during parallel processing
    /// - Pre-allocated final array (exact size, no resize)
    /// - Parallel processing for large result sets (&gt;10K rows)
    /// 
    /// The resulting List is then constructed from the array, which creates
    /// a wrapper with exact capacity (no wasted memory).
    /// </remarks>
    public List<T> Render(QueryResult queryResult)
    {
        // Delegate to existing PooledBatchMaterializer for efficient materialization
        return PooledBatchMaterializer.MaterializeToList(
            queryResult.RecordBatch,
            queryResult.SelectedIndices,
            _createItem,
            parallelOptions);
    }
}

/// <summary>
/// Materializes query results to T[] (row-oriented array).
/// Uses PooledBatchMaterializer for efficient object creation.
/// </summary>
/// <typeparam name="T">The element type to materialize.</typeparam>
/// <remarks>
/// Similar to ListRenderer but returns an array instead of a list.
/// Slightly more efficient (no List wrapper overhead) but less flexible.
/// </remarks>
/// <remarks>
/// Creates a new ArrayRenderer.
/// </remarks>
/// <param name="createItem">Function to create an item from a RecordBatch and row index.</param>
/// <param name="parallelOptions">Optional parallel execution options.</param>
internal sealed class ArrayRenderer<T>(
    Func<RecordBatch, int, T> createItem,
    ParallelQueryOptions? parallelOptions = null) : IResultRenderer<T[]>
{
    private readonly Func<RecordBatch, int, T> _createItem = createItem ?? throw new ArgumentNullException(nameof(createItem));

    /// <summary>
    /// Renders the query result to an array.
    /// </summary>
    /// <param name="queryResult">The query result to materialize.</param>
    /// <returns>An array containing materialized objects for each selected row.</returns>
    public T[] Render(QueryResult queryResult)
    {
        // Delegate to existing PooledBatchMaterializer for efficient materialization
        return PooledBatchMaterializer.MaterializeToArray(
            queryResult.RecordBatch,
            queryResult.SelectedIndices,
            _createItem,
            parallelOptions);
    }
}

/// <summary>
/// Materializes query results to IEnumerable{T} using batched enumeration.
/// More memory-efficient than List for one-time iteration.
/// </summary>
/// <typeparam name="T">The element type to materialize.</typeparam>
/// <remarks>
/// This renderer uses lazy enumeration - objects are materialized on-demand
/// as you iterate. This is more memory-efficient than List for large result
/// sets that you'll only iterate once.
/// </remarks>
/// <remarks>
/// Creates a new EnumerableRenderer.
/// </remarks>
/// <param name="createItem">Function to create an item from a RecordBatch and row index.</param>
internal sealed class EnumerableRenderer<T>(Func<RecordBatch, int, T> createItem) : IResultRenderer<IEnumerable<T>>
{
    private readonly Func<RecordBatch, int, T> _createItem = createItem ?? throw new ArgumentNullException(nameof(createItem));

    /// <summary>
    /// Renders the query result to an IEnumerable{T}.
    /// </summary>
    /// <param name="queryResult">The query result to materialize.</param>
    /// <returns>An enumerable that lazily materializes objects on iteration.</returns>
    public IEnumerable<T> Render(QueryResult queryResult)
    {
        // Use simple iterator for lazy enumeration
        var recordBatch = queryResult.RecordBatch;
        var selectedIndices = queryResult.SelectedIndices;

        foreach (var index in selectedIndices)
        {
            yield return _createItem(recordBatch, index);
        }
    }
}
