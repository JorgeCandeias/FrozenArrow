using Apache.Arrow;
using System.Buffers;
using System.Collections;

namespace ArrowCollection;

/// <summary>
/// A frozen generic collection that stores data using Apache Arrow columnar format.
/// This collection is immutable after creation and materializes items on-the-fly during enumeration.
/// </summary>
/// <typeparam name="T">The type of items in the collection. Must have a parameterless constructor.</typeparam>
/// <remarks>
/// Initializes a new instance of the ArrowCollection class.
/// </remarks>
/// <param name="recordBatch">The Arrow record batch containing the data.</param>
/// <param name="count">The number of items in the collection.</param>
/// <param name="buildStatistics">Optional build statistics collected during creation.</param>
public abstract class ArrowCollection<T>(
    RecordBatch recordBatch, 
    int count,
    ArrowCollectionBuildStatistics? buildStatistics = null) : IEnumerable<T>, IDisposable where T : new()
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
