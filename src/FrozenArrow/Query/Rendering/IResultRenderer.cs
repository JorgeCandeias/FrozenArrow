namespace FrozenArrow.Query.Rendering;

/// <summary>
/// Renders a QueryResult into a specific output format.
/// This is the Strategy interface for different output projections.
/// </summary>
/// <typeparam name="TResult">The type of the rendered result.</typeparam>
/// <remarks>
/// <para>
/// IResultRenderer decouples query execution from result materialization.
/// The query engine produces a QueryResult (selection bitmap + column references),
/// and different renderers project this into various output formats without
/// affecting the query execution logic.
/// </para>
/// 
/// <para>
/// Built-in renderers:
/// - ListRenderer{T}: Materializes to List{T} (row-oriented, existing behavior)
/// - ArrowIpcRenderer: Exports to Arrow IPC format (columnar, zero-copy)
/// - JsonStreamRenderer: Streams to JSON (columnar reading)
/// - CsvStreamRenderer: Streams to CSV (columnar reading)
/// </para>
/// 
/// <para>
/// Performance implications:
/// - Row-oriented renderers (List, Array): Must materialize objects per row
/// - Columnar renderers (Arrow IPC): Zero-copy or low-copy, no row objects
/// - Streaming renderers (JSON, CSV): Read columns directly, minimal allocations
/// </para>
/// 
/// <para>
/// Example custom renderer:
/// <code>
/// public class ParquetRenderer : IResultRenderer{RecordBatch}
/// {
///     public RecordBatch Render(in QueryResult result)
///     {
///         // Convert to Parquet-compatible RecordBatch
///         // Apply compression, encoding, etc.
///         return transformedBatch;
///     }
/// }
/// </code>
/// </para>
/// </remarks>
internal interface IResultRenderer<out TResult>
{
    /// <summary>
    /// Renders the query result into the target output format.
    /// </summary>
    /// <param name="queryResult">The logical result from query execution.</param>
    /// <returns>The materialized result in the target format.</returns>
    /// <remarks>
    /// <para>
    /// This method should be efficient and minimize allocations where possible.
    /// For large result sets, consider:
    /// - Streaming instead of buffering (JSON, CSV)
    /// - Zero-copy operations where applicable (Arrow IPC)
    /// - Parallel processing for row-oriented materialization (List, Array)
    /// </para>
    /// 
    /// <para>
    /// The QueryResult provides:
    /// - RecordBatch: The underlying columnar data
    /// - SelectedIndices: Which rows passed predicates
    /// - ProjectedColumns: Which columns are needed (null = all)
    /// - Metadata: Optional execution info for diagnostics
    /// </para>
    /// </remarks>
    TResult Render(QueryResult queryResult);
}
