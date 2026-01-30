using Apache.Arrow;
using Apache.Arrow.Types;
using System.Collections;
using System.Reflection;

namespace Colly;

/// <summary>
/// A frozen generic collection that compresses data using Apache Arrow columnar format.
/// This collection is immutable after creation and materializes items on-the-fly during enumeration.
/// </summary>
/// <typeparam name="T">The type of items in the collection. Must have a parameterless constructor.</typeparam>
public sealed class Colly<T> : IEnumerable<T>, IDisposable where T : new()
{
    private readonly RecordBatch _recordBatch;
    private readonly PropertyInfo[] _properties;
    private readonly int _count;
    private bool _disposed;

    internal Colly(RecordBatch recordBatch, PropertyInfo[] properties, int count)
    {
        _recordBatch = recordBatch ?? throw new ArgumentNullException(nameof(recordBatch));
        _properties = properties ?? throw new ArgumentNullException(nameof(properties));
        _count = count;
    }

    /// <summary>
    /// Gets the number of elements in the collection.
    /// </summary>
    public int Count => _count;

    /// <summary>
    /// Returns an enumerator that iterates through the collection.
    /// </summary>
    public IEnumerator<T> GetEnumerator()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return new CollyEnumerator(_recordBatch, _properties, _count);
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

    private sealed class CollyEnumerator(RecordBatch recordBatch, PropertyInfo[] properties, int count) : IEnumerator<T>
    {
        private int _position = -1;

        public T Current
        {
            get
            {
                if (_position < 0 || _position >= count)
                {
                    throw new InvalidOperationException("Enumerator is not positioned on a valid element.");
                }

                return MaterializeItem(_position);
            }
        }

        object IEnumerator.Current => Current!;

        public bool MoveNext()
        {
            if (_position < count - 1)
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

        private T MaterializeItem(int index)
        {
            var item = new T();

            for (int i = 0; i < properties.Length; i++)
            {
                var property = properties[i];
                var array = recordBatch.Column(i);
                var value = ExtractValue(array, index, property.PropertyType);

                if (value != null || !property.PropertyType.IsValueType || Nullable.GetUnderlyingType(property.PropertyType) != null)
                {
                    property.SetValue(item, value);
                }
            }

            return item;
        }

        private static object? ExtractValue(IArrowArray array, int index, Type targetType)
        {
            // Handle null values
            if (array.IsNull(index))
            {
                return null;
            }

            // Handle nullable types
            var underlyingType = Nullable.GetUnderlyingType(targetType);
            if (underlyingType != null)
            {
                targetType = underlyingType;
            }

            // Extract value based on Arrow array type
            return array switch
            {
                Int32Array int32Array => int32Array.GetValue(index),
                Int64Array int64Array => int64Array.GetValue(index),
                Int16Array int16Array => int16Array.GetValue(index),
                Int8Array int8Array => int8Array.GetValue(index),
                UInt32Array uint32Array => uint32Array.GetValue(index),
                UInt64Array uint64Array => uint64Array.GetValue(index),
                UInt16Array uint16Array => uint16Array.GetValue(index),
                UInt8Array uint8Array => uint8Array.GetValue(index),
                FloatArray floatArray => floatArray.GetValue(index),
                DoubleArray doubleArray => doubleArray.GetValue(index),
                BooleanArray boolArray => boolArray.GetValue(index),
                StringArray stringArray => stringArray.GetString(index),
                Date32Array date32Array => date32Array.GetValue(index).HasValue
                    ? DateTimeOffset.FromUnixTimeSeconds(date32Array.GetValue(index)!.Value * 86400L).DateTime
                    : (DateTime?)null,
                Date64Array date64Array => date64Array.GetValue(index).HasValue
                    ? DateTimeOffset.FromUnixTimeMilliseconds(date64Array.GetValue(index)!.Value).DateTime
                    : (DateTime?)null,
                TimestampArray timestampArray => ExtractTimestamp(timestampArray, index),
                _ => throw new NotSupportedException($"Array type {array.GetType().Name} is not supported.")
            };
        }

        private static DateTime ExtractTimestamp(TimestampArray array, int index)
        {
            var value = array.GetValue(index);
            if (!value.HasValue)
            {
                return default;
            }

            var timestampValue = value.Value;
            var timestampType = (TimestampType)array.Data.DataType;
            return timestampType.Unit switch
            {
                TimeUnit.Second => DateTimeOffset.FromUnixTimeSeconds(timestampValue).DateTime,
                TimeUnit.Millisecond => DateTimeOffset.FromUnixTimeMilliseconds(timestampValue).DateTime,
                TimeUnit.Microsecond => DateTimeOffset.FromUnixTimeMilliseconds(timestampValue / 1000).DateTime,
                TimeUnit.Nanosecond => DateTimeOffset.FromUnixTimeMilliseconds(timestampValue / 1000000).DateTime,
                _ => throw new NotSupportedException($"Timestamp unit {timestampType.Unit} is not supported.")
            };
        }
    }
}
