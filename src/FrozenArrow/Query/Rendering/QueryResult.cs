using Apache.Arrow;

namespace FrozenArrow.Query.Rendering;

/// <summary>
/// Represents the logical result of query execution before materialization.
/// This is the output of the query engine - a selection over columnar data.
/// </summary>
/// <remarks>
/// QueryResult decouples query execution from result rendering. The query engine
/// produces a QueryResult (selection bitmap + column references), and different
/// renderers can project this into various output formats:
/// 
/// - ListRenderer: Materializes to List{T} (row-oriented, existing behavior)
/// - ArrowIpcRenderer: Exports to Arrow IPC format (columnar, zero-copy)
/// - JsonStreamRenderer: Streams to JSON (columnar reading, no row objects)
/// - CsvStreamRenderer: Streams to CSV (columnar reading)
/// 
/// This separation enables massive performance optimizations for columnar outputs
/// where row materialization is unnecessary overhead.
/// </remarks>
/// <remarks>
/// Creates a new QueryResult.
/// </remarks>
/// <param name="recordBatch">The underlying Arrow RecordBatch containing columnar data.</param>
/// <param name="selectedIndices">The indices of selected rows. For full scans, this contains [0..N-1].</param>
/// <param name="projectedColumns">The columns that are actually needed (null = all columns).</param>
/// <param name="metadata">Optional metadata about query execution.</param>
public readonly struct QueryResult(
    RecordBatch recordBatch,
    IReadOnlyList<int> selectedIndices,
    IReadOnlyList<string>? projectedColumns = null,
    QueryExecutionMetadata? metadata = null)
{
    /// <summary>
    /// The underlying Arrow RecordBatch containing columnar data.
    /// </summary>
    public RecordBatch RecordBatch { get; } = recordBatch ?? throw new ArgumentNullException(nameof(recordBatch));

    /// <summary>
    /// The indices of selected rows.
    /// </summary>
    /// <remarks>
    /// For queries with no filters (full scan), this contains all indices [0..N-1].
    /// For filtered queries, this contains only the rows that passed predicates.
    /// </remarks>
    public IReadOnlyList<int> SelectedIndices { get; } = selectedIndices ?? throw new ArgumentNullException(nameof(selectedIndices));

    /// <summary>
    /// The columns that are actually needed for the result.
    /// Null = all columns, otherwise only the specified columns.
    /// </summary>
    /// <remarks>
    /// This enables projection pushdown optimizations:
    /// - Renderers can skip columns not in this list
    /// - Arrow IPC can export only needed columns (smaller files)
    /// - JSON/CSV can avoid reading unnecessary columns
    /// </remarks>
    public IReadOnlyList<string>? ProjectedColumns { get; } = projectedColumns;

    /// <summary>
    /// Metadata about the query execution (for debugging/profiling).
    /// </summary>
    public QueryExecutionMetadata? Metadata { get; } = metadata;

    /// <summary>
    /// Gets the number of selected rows.
    /// </summary>
    public int Count => SelectedIndices.Count;

    /// <summary>
    /// Checks if all rows are selected (no filtering applied).
    /// </summary>
    public bool IsFullScan => SelectedIndices.Count == RecordBatch.Length;

    /// <summary>
    /// Checks if all columns are projected (no column pruning applied).
    /// </summary>
    public bool IsFullProjection => ProjectedColumns == null;
}
