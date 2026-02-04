using Apache.Arrow;
using Apache.Arrow.Ipc;
using FrozenArrow.Query;
using System.Buffers;
using System.Collections;

namespace FrozenArrow;

/// <summary>
/// A frozen generic collection that stores data using Apache Arrow columnar format.
/// This collection is immutable after creation and materializes items on-the-fly during enumeration.
/// </summary>
/// <typeparam name="T">The type of items in the collection.</typeparam>
/// <remarks>
/// <para>
/// Initializes a new instance of the FrozenArrow class.
/// </para>
/// <para>
/// <strong>Important:</strong> Constructors are bypassed during item reconstruction. Items are created
/// using <see cref="System.Runtime.CompilerServices.RuntimeHelpers.GetUninitializedObject"/> for classes
/// and <c>default(T)</c> for structs, then fields are set directly. This means:
/// <list type="bullet">
///   <item>Positional records work without a parameterless constructor</item>
///   <item>Constructor validation logic is not executed</item>
///   <item>Field initializers are not executed</item>
/// </list>
/// This behavior is by design, as FrozenArrow expects types to be pure data containers.
/// </para>
/// </remarks>
/// <param name="recordBatch">The Arrow record batch containing the data.</param>
/// <param name="count">The number of items in the collection.</param>
/// <param name="buildStatistics">Optional build statistics collected during creation.</param>
public abstract class FrozenArrow<T>(
    RecordBatch recordBatch, 
    int count,
    FrozenArrowBuildStatistics? buildStatistics = null) : IEnumerable<T>, IDisposable
{
    private readonly RecordBatch _recordBatch = recordBatch ?? throw new ArgumentNullException(nameof(recordBatch));
    private readonly int _count = count;
    private readonly FrozenArrowBuildStatistics? _buildStatistics = buildStatistics;
    private bool _disposed;

    /// <summary>
    /// Query plan cache shared by all queries against this FrozenArrow instance.
    /// This ensures that repeated calls to AsQueryable() benefit from cached query plans.
    /// </summary>
    private readonly QueryPlanCache _queryPlanCache = new();

    /// <summary>
    /// Gets the number of elements in the collection.
    /// </summary>
    public int Count => _count;

    /// <summary>
    /// Gets the build statistics collected during creation of this collection.
    /// May be null if statistics collection was disabled.
    /// </summary>
    public FrozenArrowBuildStatistics? BuildStatistics => _buildStatistics;

    /// <summary>
    /// Gets the query plan cache statistics for monitoring cache performance.
    /// </summary>
    /// <remarks>
    /// This cache is shared across all queries created via AsQueryable() on this instance.
    /// The cache eliminates repeated expression tree analysis for the same query patterns.
    /// </remarks>
    public CacheStatistics QueryPlanCacheStatistics => _queryPlanCache.Statistics;

    /// <summary>
    /// Gets the number of cached query plans for this instance.
    /// </summary>
    public int CachedQueryPlanCount => _queryPlanCache.Count;

    /// <summary>
    /// Clears the query plan cache for this instance.
    /// </summary>
    public void ClearQueryPlanCache() => _queryPlanCache.Clear();

    /// <summary>
    /// Creates an item of type T from the record batch at the specified index.
    /// This method is implemented by generated code for optimal performance.
    /// </summary>
    /// <param name="recordBatch">The Arrow record batch.</param>
    /// <param name="index">The index of the item to create.</param>
    /// <returns>A new instance of T populated with data from the record batch.</returns>
    protected abstract T CreateItem(RecordBatch recordBatch, int index);

    /// <summary>
    /// Internal accessor for the record batch. Used by the query engine to avoid reflection overhead.
    /// </summary>
    internal RecordBatch RecordBatch => _recordBatch;

    /// <summary>
    /// Internal accessor for creating items. Used by the query engine to avoid reflection overhead.
    /// </summary>
    internal T CreateItemInternal(RecordBatch recordBatch, int index) => CreateItem(recordBatch, index);

    /// <summary>
    /// Internal accessor for the query plan cache. Used by the query provider.
    /// </summary>
    internal QueryPlanCache QueryPlanCache => _queryPlanCache;

    /// <summary>
    /// Returns an enumerator that iterates through the collection.
    /// </summary>
    public IEnumerator<T> GetEnumerator()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return new FrozenArrowEnumerator(this);
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    /// <summary>
    /// Releases the resources used by this collection.
    /// </summary>
    public void Dispose()
    {
        if (!_disposed)
        {
            _recordBatch?.Dispose();
            _disposed = true;
        }
    }

    #region Serialization - Writing

    /// <summary>
    /// Writes the collection to the specified buffer writer using Arrow IPC format.
    /// </summary>
    /// <param name="writer">The buffer writer to write to.</param>
    /// <param name="options">Optional write options. If null, default options are used.</param>
    /// <exception cref="ObjectDisposedException">The collection has been disposed.</exception>
    public void WriteTo(IBufferWriter<byte> writer, ArrowWriteOptions? options = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(writer);

        using var stream = new BufferWriterStream(writer);
        WriteToStreamCore(stream, options ?? ArrowWriteOptions.Default);
    }

    /// <summary>
    /// Asynchronously writes the collection to the specified stream using Arrow IPC format.
    /// </summary>
    /// <param name="stream">The stream to write to.</param>
    /// <param name="options">Optional write options. If null, default options are used.</param>
    /// <param name="cancellationToken">A cancellation token to observe.</param>
    /// <exception cref="ObjectDisposedException">The collection has been disposed.</exception>
    public async Task WriteToAsync(Stream stream, ArrowWriteOptions? options = null, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(stream);

        await WriteToStreamCoreAsync(stream, options ?? ArrowWriteOptions.Default, cancellationToken).ConfigureAwait(false);
    }

    private void WriteToStreamCore(Stream stream, ArrowWriteOptions options)
    {
        ICompressionCodecFactory? codecFactory = null;
        if (options.CompressionCodec.HasValue)
        {
            CompressionInitializer.EnsureInitialized();
            codecFactory = CompressionInitializer.CodecFactory;
        }

        var ipcOptions = new IpcOptions 
        { 
            CompressionCodec = options.CompressionCodec,
            CompressionCodecFactory = codecFactory
        };
        
        using var writer = new ArrowStreamWriter(stream, _recordBatch.Schema, leaveOpen: true, ipcOptions);
        writer.WriteRecordBatch(_recordBatch);
        writer.WriteEnd();
    }

    private async Task WriteToStreamCoreAsync(Stream stream, ArrowWriteOptions options, CancellationToken cancellationToken)
    {
        ICompressionCodecFactory? codecFactory = null;
        if (options.CompressionCodec.HasValue)
        {
            CompressionInitializer.EnsureInitialized();
            codecFactory = CompressionInitializer.CodecFactory;
        }

        var ipcOptions = new IpcOptions 
        { 
            CompressionCodec = options.CompressionCodec,
            CompressionCodecFactory = codecFactory
        };
        
        using var writer = new ArrowStreamWriter(stream, _recordBatch.Schema, leaveOpen: true, ipcOptions);
        await writer.WriteRecordBatchAsync(_recordBatch, cancellationToken).ConfigureAwait(false);
        await writer.WriteEndAsync(cancellationToken).ConfigureAwait(false);
    }

    #endregion

    #region Serialization - Reading

    /// <summary>
    /// Reads an FrozenArrow from the specified byte span using Arrow IPC format.
    /// </summary>
    /// <param name="data">The byte span containing the serialized data.</param>
    /// <param name="options">Optional read options. If null, default options are used.</param>
    /// <returns>A new FrozenArrow populated from the serialized data.</returns>
    public static FrozenArrow<T> ReadFrom(ReadOnlySpan<byte> data, ArrowReadOptions? options = null)
    {
        // Copy span to array since we need a stream for Arrow IPC reader
        var array = data.ToArray();
        using var stream = new MemoryStream(array, writable: false);
        return ReadFromStreamCore(stream, options ?? ArrowReadOptions.Default);
    }

    /// <summary>
    /// Reads an FrozenArrow from the specified byte sequence using Arrow IPC format.
    /// </summary>
    /// <param name="data">The byte sequence containing the serialized data.</param>
    /// <param name="options">Optional read options. If null, default options are used.</param>
    /// <returns>A new FrozenArrow populated from the serialized data.</returns>
    /// <remarks>
    /// This overload is optimized for pipeline scenarios where data arrives as a sequence of segments.
    /// </remarks>
    public static FrozenArrow<T> ReadFrom(ReadOnlySequence<byte> data, ArrowReadOptions? options = null)
    {
        // Convert sequence to contiguous array for Arrow IPC reader
        var array = data.ToArray();
        using var stream = new MemoryStream(array, writable: false);
        return ReadFromStreamCore(stream, options ?? ArrowReadOptions.Default);
    }

    /// <summary>
    /// Asynchronously reads an FrozenArrow from the specified stream using Arrow IPC format.
    /// </summary>
    /// <param name="stream">The stream containing the serialized data.</param>
    /// <param name="options">Optional read options. If null, default options are used.</param>
    /// <param name="cancellationToken">A cancellation token to observe.</param>
    /// <returns>A task that represents the asynchronous read operation.</returns>
    public static async Task<FrozenArrow<T>> ReadFromAsync(Stream stream, ArrowReadOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream);
        return await ReadFromStreamCoreAsync(stream, options ?? ArrowReadOptions.Default, cancellationToken).ConfigureAwait(false);
    }

    private static FrozenArrow<T> ReadFromStreamCore(Stream stream, ArrowReadOptions options)
    {
        // Initialize compression codecs in case the stream contains compressed data
        CompressionInitializer.EnsureInitialized();

        using var reader = new ArrowStreamReader(stream, CompressionInitializer.CodecFactory, leaveOpen: true);
        var recordBatch = reader.ReadNextRecordBatch() 
            ?? throw new InvalidOperationException("No record batch found in the stream.");
        
        return CreateFromRecordBatch(recordBatch, options);
    }

    private static async Task<FrozenArrow<T>> ReadFromStreamCoreAsync(Stream stream, ArrowReadOptions options, CancellationToken cancellationToken)
    {
        // Initialize compression codecs in case the stream contains compressed data
        CompressionInitializer.EnsureInitialized();

        using var reader = new ArrowStreamReader(stream, CompressionInitializer.CodecFactory, leaveOpen: true);
        var recordBatch = await reader.ReadNextRecordBatchAsync(cancellationToken).ConfigureAwait(false)
            ?? throw new InvalidOperationException("No record batch found in the stream.");
        
        return CreateFromRecordBatch(recordBatch, options);
    }

    private static FrozenArrow<T> CreateFromRecordBatch(RecordBatch recordBatch, ArrowReadOptions options)
    {
        if (!FrozenArrowFactoryRegistry.TryGetDeserializationFactory<T>(out var factory))
        {
            throw new InvalidOperationException(
                $"No deserialization factory registered for type '{typeof(T).FullName}'. " +
                $"Ensure the type is annotated with [ArrowRecord] and the source generator has run.");
        }

        return factory!(recordBatch, options);
    }

    #endregion

    #region Helper Types

    /// <summary>
    /// A stream wrapper around an IBufferWriter for writing.
    /// </summary>
    private sealed class BufferWriterStream(IBufferWriter<byte> writer) : Stream
    {
        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => throw new NotSupportedException();
        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override void Flush() { }

        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count)
        {
            var span = writer.GetSpan(count);
            buffer.AsSpan(offset, count).CopyTo(span);
            writer.Advance(count);
        }

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            var span = writer.GetSpan(buffer.Length);
            buffer.CopyTo(span);
            writer.Advance(buffer.Length);
        }
    }

    #endregion

    private sealed class FrozenArrowEnumerator(FrozenArrow<T> collection) : IEnumerator<T>
    {
        private int _position = -1;

        public T Current
        {
            get
            {
                if (_position < 0 || _position >= collection._count)
                {
                    throw new InvalidOperationException("Enumerator is not positioned on a valid element.");
                }

                return collection.CreateItem(collection._recordBatch, _position);
            }
        }

        object IEnumerator.Current => Current!;

        public bool MoveNext()
        {
            if (_position < collection._count - 1)
            {
                _position++;
                return true;
            }
            return false;
        }

        public void Reset()
        {
            _position = -1;
        }

        public void Dispose()
        {
            // Nothing to dispose in the enumerator itself
        }
    }
}
