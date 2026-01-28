using System.Reflection;
using Apache.Arrow;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Colly;

/// <summary>
/// Extension methods for converting IEnumerable to Colly collections.
/// </summary>
public static class EnumerableExtensions
{
    /// <summary>
    /// Converts an IEnumerable to a Colly frozen collection.
    /// The collection compresses the data using Apache Arrow columnar format.
    /// </summary>
    /// <typeparam name="T">The type of items in the collection.</typeparam>
    /// <param name="source">The source enumerable.</param>
    /// <returns>A frozen Colly collection.</returns>
    public static Colly<T> ToColly<T>(this IEnumerable<T> source) where T : new()
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        // Get all public instance properties
        var properties = typeof(T)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanRead && p.CanWrite)
            .ToArray();

        if (properties.Length == 0)
        {
            throw new InvalidOperationException($"Type {typeof(T).Name} has no readable and writable public instance properties.");
        }

        // Materialize the source
        var items = source.ToList();
        var count = items.Count;

        // Build Arrow schema
        var fields = new List<Field>();
        foreach (var property in properties)
        {
            var arrowType = GetArrowType(property.PropertyType);
            fields.Add(new Field(property.Name, arrowType, IsNullable(property.PropertyType)));
        }
        var schema = new Schema(fields, null);

        // Build Arrow arrays for each property
        var arrays = new List<IArrowArray>();
        var allocator = new NativeMemoryAllocator();

        foreach (var property in properties)
        {
            var array = BuildArray(items, property, allocator);
            arrays.Add(array);
        }

        // Create record batch
        var recordBatch = new RecordBatch(schema, arrays, count);

        return new Colly<T>(recordBatch, properties, count);
    }

    private static IArrowType GetArrowType(Type type)
    {
        // Handle nullable types
        var underlyingType = Nullable.GetUnderlyingType(type);
        if (underlyingType != null)
        {
            type = underlyingType;
        }

        if (type == typeof(int))
            return Int32Type.Default;
        if (type == typeof(long))
            return Int64Type.Default;
        if (type == typeof(short))
            return Int16Type.Default;
        if (type == typeof(sbyte))
            return Int8Type.Default;
        if (type == typeof(uint))
            return UInt32Type.Default;
        if (type == typeof(ulong))
            return UInt64Type.Default;
        if (type == typeof(ushort))
            return UInt16Type.Default;
        if (type == typeof(byte))
            return UInt8Type.Default;
        if (type == typeof(float))
            return FloatType.Default;
        if (type == typeof(double))
            return DoubleType.Default;
        if (type == typeof(bool))
            return BooleanType.Default;
        if (type == typeof(string))
            return StringType.Default;
        if (type == typeof(DateTime))
            return new TimestampType(TimeUnit.Millisecond, TimeZoneInfo.Utc);

        throw new NotSupportedException($"Type {type.Name} is not supported.");
    }

    private static bool IsNullable(Type type)
    {
        return !type.IsValueType || Nullable.GetUnderlyingType(type) != null;
    }

    private static IArrowArray BuildArray<T>(List<T> items, PropertyInfo property, MemoryAllocator allocator)
    {
        var propertyType = property.PropertyType;
        var underlyingType = Nullable.GetUnderlyingType(propertyType) ?? propertyType;

        if (underlyingType == typeof(int))
            return BuildInt32Array(items, property, allocator);
        if (underlyingType == typeof(long))
            return BuildInt64Array(items, property, allocator);
        if (underlyingType == typeof(short))
            return BuildInt16Array(items, property, allocator);
        if (underlyingType == typeof(sbyte))
            return BuildInt8Array(items, property, allocator);
        if (underlyingType == typeof(uint))
            return BuildUInt32Array(items, property, allocator);
        if (underlyingType == typeof(ulong))
            return BuildUInt64Array(items, property, allocator);
        if (underlyingType == typeof(ushort))
            return BuildUInt16Array(items, property, allocator);
        if (underlyingType == typeof(byte))
            return BuildUInt8Array(items, property, allocator);
        if (underlyingType == typeof(float))
            return BuildFloatArray(items, property, allocator);
        if (underlyingType == typeof(double))
            return BuildDoubleArray(items, property, allocator);
        if (underlyingType == typeof(bool))
            return BuildBooleanArray(items, property, allocator);
        if (underlyingType == typeof(string))
            return BuildStringArray(items, property, allocator);
        if (underlyingType == typeof(DateTime))
            return BuildTimestampArray(items, property, allocator);

        throw new NotSupportedException($"Type {underlyingType.Name} is not supported.");
    }

    private static Int32Array BuildInt32Array<T>(List<T> items, PropertyInfo property, MemoryAllocator allocator)
    {
        var builder = new Int32Array.Builder().Reserve(items.Count);
        foreach (var item in items)
        {
            var value = property.GetValue(item);
            if (value == null)
                builder.AppendNull();
            else
                builder.Append((int)value);
        }
        return builder.Build(allocator);
    }

    private static Int64Array BuildInt64Array<T>(List<T> items, PropertyInfo property, MemoryAllocator allocator)
    {
        var builder = new Int64Array.Builder().Reserve(items.Count);
        foreach (var item in items)
        {
            var value = property.GetValue(item);
            if (value == null)
                builder.AppendNull();
            else
                builder.Append((long)value);
        }
        return builder.Build(allocator);
    }

    private static Int16Array BuildInt16Array<T>(List<T> items, PropertyInfo property, MemoryAllocator allocator)
    {
        var builder = new Int16Array.Builder().Reserve(items.Count);
        foreach (var item in items)
        {
            var value = property.GetValue(item);
            if (value == null)
                builder.AppendNull();
            else
                builder.Append((short)value);
        }
        return builder.Build(allocator);
    }

    private static Int8Array BuildInt8Array<T>(List<T> items, PropertyInfo property, MemoryAllocator allocator)
    {
        var builder = new Int8Array.Builder().Reserve(items.Count);
        foreach (var item in items)
        {
            var value = property.GetValue(item);
            if (value == null)
                builder.AppendNull();
            else
                builder.Append((sbyte)value);
        }
        return builder.Build(allocator);
    }

    private static UInt32Array BuildUInt32Array<T>(List<T> items, PropertyInfo property, MemoryAllocator allocator)
    {
        var builder = new UInt32Array.Builder().Reserve(items.Count);
        foreach (var item in items)
        {
            var value = property.GetValue(item);
            if (value == null)
                builder.AppendNull();
            else
                builder.Append((uint)value);
        }
        return builder.Build(allocator);
    }

    private static UInt64Array BuildUInt64Array<T>(List<T> items, PropertyInfo property, MemoryAllocator allocator)
    {
        var builder = new UInt64Array.Builder().Reserve(items.Count);
        foreach (var item in items)
        {
            var value = property.GetValue(item);
            if (value == null)
                builder.AppendNull();
            else
                builder.Append((ulong)value);
        }
        return builder.Build(allocator);
    }

    private static UInt16Array BuildUInt16Array<T>(List<T> items, PropertyInfo property, MemoryAllocator allocator)
    {
        var builder = new UInt16Array.Builder().Reserve(items.Count);
        foreach (var item in items)
        {
            var value = property.GetValue(item);
            if (value == null)
                builder.AppendNull();
            else
                builder.Append((ushort)value);
        }
        return builder.Build(allocator);
    }

    private static UInt8Array BuildUInt8Array<T>(List<T> items, PropertyInfo property, MemoryAllocator allocator)
    {
        var builder = new UInt8Array.Builder().Reserve(items.Count);
        foreach (var item in items)
        {
            var value = property.GetValue(item);
            if (value == null)
                builder.AppendNull();
            else
                builder.Append((byte)value);
        }
        return builder.Build(allocator);
    }

    private static FloatArray BuildFloatArray<T>(List<T> items, PropertyInfo property, MemoryAllocator allocator)
    {
        var builder = new FloatArray.Builder().Reserve(items.Count);
        foreach (var item in items)
        {
            var value = property.GetValue(item);
            if (value == null)
                builder.AppendNull();
            else
                builder.Append((float)value);
        }
        return builder.Build(allocator);
    }

    private static DoubleArray BuildDoubleArray<T>(List<T> items, PropertyInfo property, MemoryAllocator allocator)
    {
        var builder = new DoubleArray.Builder().Reserve(items.Count);
        foreach (var item in items)
        {
            var value = property.GetValue(item);
            if (value == null)
                builder.AppendNull();
            else
                builder.Append((double)value);
        }
        return builder.Build(allocator);
    }

    private static BooleanArray BuildBooleanArray<T>(List<T> items, PropertyInfo property, MemoryAllocator allocator)
    {
        var builder = new BooleanArray.Builder().Reserve(items.Count);
        foreach (var item in items)
        {
            var value = property.GetValue(item);
            if (value == null)
                builder.AppendNull();
            else
                builder.Append((bool)value);
        }
        return builder.Build(allocator);
    }

    private static StringArray BuildStringArray<T>(List<T> items, PropertyInfo property, MemoryAllocator allocator)
    {
        var builder = new StringArray.Builder();
        foreach (var item in items)
        {
            var value = property.GetValue(item);
            if (value == null)
                builder.AppendNull();
            else
                builder.Append((string)value);
        }
        return builder.Build(allocator);
    }

    private static TimestampArray BuildTimestampArray<T>(List<T> items, PropertyInfo property, MemoryAllocator allocator)
    {
        var builder = new TimestampArray.Builder(new TimestampType(TimeUnit.Millisecond, TimeZoneInfo.Utc))
            .Reserve(items.Count);
        
        foreach (var item in items)
        {
            var value = property.GetValue(item);
            if (value == null)
                builder.AppendNull();
            else
            {
                var dateTime = (DateTime)value;
                var dateTimeOffset = new DateTimeOffset(dateTime, TimeSpan.Zero);
                builder.Append(dateTimeOffset);
            }
        }
        return builder.Build(allocator);
    }
}
