using Apache.Arrow;
using Apache.Arrow.Ipc;
using FrozenArrow.Query.Rendering;

namespace FrozenArrow.Query;

/// <summary>
/// Extension methods for rendering query results to Arrow IPC format.
/// Phase 2: Columnar output rendering.
/// </summary>
public static class ArrowIpcRenderingExtensions
{
    /// <summary>
    /// Renders query results directly to Arrow RecordBatch format (columnar).
    /// This is a ZERO-COPY or LOW-COPY operation - no row materialization!
    /// </summary>
    /// <typeparam name="T">The element type of the query.</typeparam>
    /// <param name="query">The query to execute and render.</param>
    /// <returns>A RecordBatch containing the filtered/projected data.</returns>
    /// <remarks>
    /// <para>
    /// Performance characteristics:
    /// - Full scan: ZERO-COPY (returns original RecordBatch)
    /// - Projection only: LOW-COPY (~1µs per column)
    /// - Filtering: COLUMNAR COPY (~1µs per 1000 rows per column)
    /// 
    /// Expected speedup vs .ToList():
    /// - Full scan: ~50-100x faster (reference copy vs materialization)
    /// - Filtered (50% selectivity): ~10-20x faster
    /// - Filtered (10% selectivity): ~5-10x faster
    /// </para>
    /// 
    /// <para>
    /// Use cases:
    /// - Export to Arrow IPC files (.arrow, .feather)
    /// - Service-to-service data exchange (Arrow Flight)
    /// - Data lake exports (Arrow to Parquet conversion)
    /// - Analytics integration (Spark, DuckDB, Polars)
    /// </para>
    /// 
    /// <para>
    /// Example:
    /// <code>
    /// var batch = collection
    ///     .AsQueryable()
    ///     .Where(x => x.Age > 30)
    ///     .ToArrowBatch();
    /// 
    /// // Write to file
    /// using var stream = File.Create("data.arrow");
    /// batch.WriteArrowIpc(stream);
    /// </code>
    /// </para>
    /// </remarks>
    /// <exception cref="NotSupportedException">
    /// Thrown if the query is not an ArrowQuery (requires columnar backing data).
    /// </exception>
    public static RecordBatch ToArrowBatch<T>(this IQueryable<T> query)
    {
        if (query is not ArrowQuery<T> arrowQuery)
        {
            throw new NotSupportedException(
                "ToArrowBatch requires an ArrowQuery. Use collection.AsQueryable() to create one.");
        }

        // Get the underlying query provider
        var provider = (ArrowQueryProvider)arrowQuery.Provider;

        // Execute query to get QueryResult (selection + projection info)
        var queryResult = provider.ExecuteToQueryResult(arrowQuery.Expression);

        // Render using ArrowIpcRenderer (columnar operations only!)
        var renderer = new ArrowIpcRenderer();
        return renderer.Render(queryResult);
    }

    /// <summary>
    /// Writes query results directly to an Arrow IPC stream.
    /// This is the standard Arrow IPC format used by most Arrow implementations.
    /// </summary>
    /// <typeparam name="T">The element type of the query.</typeparam>
    /// <param name="query">The query to execute and render.</param>
    /// <param name="stream">The stream to write to (must be writable).</param>
    /// <param name="leaveOpen">Whether to leave the stream open after writing (default: false).</param>
    /// <remarks>
    /// <para>
    /// The Arrow IPC format consists of:
    /// 1. Schema message (describes column types and metadata)
    /// 2. RecordBatch messages (actual data in columnar format)
    /// 3. End-of-stream marker
    /// </para>
    /// 
    /// <para>
    /// This format is:
    /// - Language-agnostic (readable by Python, Java, C++, Rust, etc.)
    /// - Zero-copy (can be memory-mapped for instant loading)
    /// - Self-describing (schema is embedded)
    /// - Efficient (columnar storage, no parsing overhead)
    /// </para>
    /// 
    /// <para>
    /// Example:
    /// <code>
    /// var query = collection
    ///     .AsQueryable()
    ///     .Where(x => x.Status == "Active");
    /// 
    /// using var fileStream = File.Create("output.arrow");
    /// query.WriteArrowIpc(fileStream);
    /// 
    /// // Can now be read by any Arrow implementation:
    /// // Python: pyarrow.ipc.open_stream()
    /// // Java: ArrowFileReader
    /// // C++: arrow::ipc::RecordBatchFileReader
    /// </code>
    /// </para>
    /// </remarks>
    public static void WriteArrowIpc<T>(this IQueryable<T> query, Stream stream, bool leaveOpen = false)
    {
        ArgumentNullException.ThrowIfNull(stream);

        if (!stream.CanWrite)
        {
            throw new ArgumentException("Stream must be writable.", nameof(stream));
        }

        // Get the RecordBatch to write
        var batch = query.ToArrowBatch();

        // Write using Arrow IPC writer
        using var writer = new ArrowStreamWriter(stream, batch.Schema, leaveOpen);
        writer.WriteRecordBatch(batch);
        writer.WriteEnd();
    }

    /// <summary>
    /// Writes a RecordBatch to an Arrow IPC stream.
    /// This is a convenience method for RecordBatch instances.
    /// </summary>
    /// <param name="batch">The RecordBatch to write.</param>
    /// <param name="stream">The stream to write to (must be writable).</param>
    /// <param name="leaveOpen">Whether to leave the stream open after writing (default: false).</param>
    public static void WriteArrowIpc(this RecordBatch batch, Stream stream, bool leaveOpen = false)
    {
        ArgumentNullException.ThrowIfNull(batch);
        ArgumentNullException.ThrowIfNull(stream);

        if (!stream.CanWrite)
        {
            throw new ArgumentException("Stream must be writable.", nameof(stream));
        }

        using var writer = new ArrowStreamWriter(stream, batch.Schema, leaveOpen);
        writer.WriteRecordBatch(batch);
        writer.WriteEnd();
    }

    /// <summary>
    /// Writes query results to an Arrow IPC file format.
    /// This format includes a footer for random access and is slightly larger than stream format.
    /// </summary>
    /// <typeparam name="T">The element type of the query.</typeparam>
    /// <param name="query">The query to execute and render.</param>
    /// <param name="stream">The stream to write to (must be writable and seekable).</param>
    /// <param name="leaveOpen">Whether to leave the stream open after writing (default: false).</param>
    /// <remarks>
    /// <para>
    /// The Arrow IPC file format (also called Feather v2) includes:
    /// - Schema
    /// - RecordBatch(es)
    /// - Footer with metadata and offsets for random access
    /// </para>
    /// 
    /// <para>
    /// Use file format when:
    /// - You need random access to data
    /// - You want to memory-map the file
    /// - You're storing data for later use
    /// 
    /// Use stream format when:
    /// - Streaming over network
    /// - Processing data once
    /// - Lower overhead is important
    /// </para>
    /// </remarks>
    public static void WriteArrowFile<T>(this IQueryable<T> query, Stream stream, bool leaveOpen = false)
    {
        ArgumentNullException.ThrowIfNull(stream);

        if (!stream.CanWrite)
        {
            throw new ArgumentException("Stream must be writable.", nameof(stream));
        }

        if (!stream.CanSeek)
        {
            throw new ArgumentException("Stream must be seekable for Arrow file format. Use WriteArrowIpc() for non-seekable streams.", nameof(stream));
        }

        var batch = query.ToArrowBatch();

        using var writer = new ArrowFileWriter(stream, batch.Schema, leaveOpen);
        writer.WriteRecordBatch(batch);
        writer.WriteEnd();
    }
}
