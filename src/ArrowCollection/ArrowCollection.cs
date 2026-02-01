using Apache.Arrow;
using Apache.Arrow.Ipc;
using System.Buffers;
using System.Collections;

namespace ArrowCollection;

/// <summary>
/// A frozen generic collection that stores data using Apache Arrow columnar format.
/// This collection is immutable after creation and materializes items on-the-fly during enumeration.
/// </summary>
/// <typeparam name="T">The type of items in the collection.</typeparam>
/// <remarks>
/// <para>
/// Initializes a new instance of the ArrowCollection class.
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
/// This behavior is by design, as ArrowCollection expects types to be pure data containers.
/// </para>
/// </remarks>
/// <param name="recordBatch">The Arrow record batch containing the data.</param>
/// <param name="count">The number of items in the collection.</param>
/// <param name="buildStatistics">Optional build statistics collected during creation.</param>
public abstract class ArrowCollection<T>(
    RecordBatch recordBatch, 
    int count,
    ArrowCollectionBuildStatistics? buildStatistics = null) : IEnumerable<T>, IDisposable
{
    private readonly RecordBatch _recordBatch = recordBatch ?? throw new ArgumentNullException(nameof(recordBatch));
    private readonly int _count = count;
    private readonly ArrowCollectionBuildStatistics? _buildStatistics = buildStatistics;
    private bool _disposed;

    /// <summary>
    /// Gets the number of elements in the collection.
    /// </summary>
    public int Count => _count;

    /// <summary>
    /// Gets the build statistics collected during creation of this collection.
    /// May be null if statistics collection was disabled.
    /// </summary>
    public ArrowCollectionBuildStatistics? BuildStatistics => _buildStatistics;

    /// <summary>
    /// Creates an item of type T from the record batch at the specified index.
    /// This method is implemented by generated code for optimal performance.
    /// </summary>
    /// <param name="recordBatch">The Arrow record batch.</param>
    /// <param name="index">The index of the item to create.</param>
    /// <returns>A new instance of T populated with data from the record batch.</returns>
    protected abstract T CreateItem(RecordBatch recordBatch, int index);

    /// <summary>
    /// Returns an enumerator that iterates through the collection.
    /// </summary>
    public IEnumerator<T> GetEnumerator()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return new ArrowCollectionEnumerator(this);
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
        WriteToStreamCore(stream);
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

        await WriteToStreamCoreAsync(stream, cancellationToken).ConfigureAwait(false);
    }

    private void WriteToStreamCore(Stream stream)
    {
        using var writer = new ArrowStreamWriter(stream, _recordBatch.Schema, leaveOpen: true);
        writer.WriteRecordBatch(_recordBatch);
        writer.WriteEnd();
    }

    private async Task WriteToStreamCoreAsync(Stream stream, CancellationToken cancellationToken)
    {
        using var writer = new ArrowStreamWriter(stream, _recordBatch.Schema, leaveOpen: true);
        await writer.WriteRecordBatchAsync(_recordBatch, cancellationToken).ConfigureAwait(false);
        await writer.WriteEndAsync(cancellationToken).ConfigureAwait(false);
    }

    #endregion

    #region Serialization - Reading

    /// <summary>
    /// Reads an ArrowCollection from the specified byte span using Arrow IPC format.
    /// </summary>
    /// <param name="data">The byte span containing the serialized data.</param>
    /// <param name="options">Optional read options. If null, default options are used.</param>
    /// <returns>A new ArrowCollection populated from the serialized data.</returns>
    public static ArrowCollection<T> ReadFrom(ReadOnlySpan<byte> data, ArrowReadOptions? options = null)
    {
        // Copy span to array since we need a stream for Arrow IPC reader
        var array = data.ToArray();
        using var stream = new MemoryStream(array, writable: false);
        return ReadFromStreamCore(stream, options ?? ArrowReadOptions.Default);
    }

    /// <summary>
    /// Reads an ArrowCollection from the specified byte sequence using Arrow IPC format.
    /// </summary>
    /// <param name="data">The byte sequence containing the serialized data.</param>
    /// <param name="options">Optional read options. If null, default options are used.</param>
    /// <returns>A new ArrowCollection populated from the serialized data.</returns>
    /// <remarks>
    /// This overload is optimized for pipeline scenarios where data arrives as a sequence of segments.
    /// </remarks>
    public static ArrowCollection<T> ReadFrom(ReadOnlySequence<byte> data, ArrowReadOptions? options = null)
    {
        // Convert sequence to contiguous array for Arrow IPC reader
        var array = data.ToArray();
        using var stream = new MemoryStream(array, writable: false);
        return ReadFromStreamCore(stream, options ?? ArrowReadOptions.Default);
    }

    /// <summary>
    /// Asynchronously reads an ArrowCollection from the specified stream using Arrow IPC format.
    /// </summary>
    /// <param name="stream">The stream containing the serialized data.</param>
    /// <param name="options">Optional read options. If null, default options are used.</param>
    /// <param name="cancellationToken">A cancellation token to observe.</param>
    /// <returns>A task that represents the asynchronous read operation.</returns>
    public static async Task<ArrowCollection<T>> ReadFromAsync(Stream stream, ArrowReadOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream);
        return await ReadFromStreamCoreAsync(stream, options ?? ArrowReadOptions.Default, cancellationToken).ConfigureAwait(false);
    }

    private static ArrowCollection<T> ReadFromStreamCore(Stream stream, ArrowReadOptions options)
    {
        using var reader = new ArrowStreamReader(stream, leaveOpen: true);
        var recordBatch = reader.ReadNextRecordBatch() 
            ?? throw new InvalidOperationException("No record batch found in the stream.");
        
        return CreateFromRecordBatch(recordBatch, options);
    }

    private static async Task<ArrowCollection<T>> ReadFromStreamCoreAsync(Stream stream, ArrowReadOptions options, CancellationToken cancellationToken)
    {
        using var reader = new ArrowStreamReader(stream, leaveOpen: true);
        var recordBatch = await reader.ReadNextRecordBatchAsync(cancellationToken).ConfigureAwait(false)
            ?? throw new InvalidOperationException("No record batch found in the stream.");
        
        return CreateFromRecordBatch(recordBatch, options);
    }

    private static ArrowCollection<T> CreateFromRecordBatch(RecordBatch recordBatch, ArrowReadOptions options)
    {
        if (!ArrowCollectionFactoryRegistry.TryGetDeserializationFactory<T>(out var factory))
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

    private sealed class ArrowCollectionEnumerator(ArrowCollection<T> collection) : IEnumerator<T>
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
