using Apache.Arrow;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace ArrowCollection;

/// <summary>
/// Builds optimally-encoded Arrow arrays from collected values.
/// This builder selects the best encoding based on column statistics:
/// - RLE for sorted data with long runs (currently falls back to dictionary until Arrow RLE support is available)
/// - Dictionary for low-cardinality data
/// - Primitive for everything else
/// </summary>
/// <remarks>
/// Note: Run-End Encoding (RLE) requires Apache Arrow 13.0+ which introduced RunEndEncodedArray.
/// Until we upgrade, RLE candidates will use dictionary encoding as the next best option.
/// </remarks>
public static class RunLengthEncodedArrayBuilder
{
    /// <summary>
    /// Default run ratio threshold below which RLE would be used (if available).
    /// </summary>
    public const double DefaultRleThreshold = 0.1;

    /// <summary>
    /// Builds a string array with optimal encoding based on statistics.
    /// Priority: RLE (if supported) > Dictionary > Primitive
    /// </summary>
    public static IArrowArray BuildStringArray(
        IReadOnlyList<string?> values,
        ColumnStatistics? statistics,
        MemoryAllocator allocator,
        double rleThreshold = DefaultRleThreshold)
    {
        // RLE would be used here if Apache.Arrow version supported it
        // For now, fall back to dictionary encoding which is the next best option
        // Dictionary encoding is particularly effective for RLE candidates since they have low cardinality by definition
        return DictionaryArrayBuilder.BuildStringArray(values, statistics, allocator);
    }

    /// <summary>
    /// Builds an int32 array with optimal encoding based on statistics.
    /// </summary>
    public static IArrowArray BuildInt32Array(
        IReadOnlyList<int> values,
        ColumnStatistics? statistics,
        MemoryAllocator allocator,
        double rleThreshold = DefaultRleThreshold)
    {
        return DictionaryArrayBuilder.BuildInt32Array(values, statistics, allocator);
    }

    /// <summary>
    /// Builds a double array with optimal encoding based on statistics.
    /// </summary>
    public static IArrowArray BuildDoubleArray(
        IReadOnlyList<double> values,
        ColumnStatistics? statistics,
        MemoryAllocator allocator,
        double rleThreshold = DefaultRleThreshold)
    {
        return DictionaryArrayBuilder.BuildDoubleArray(values, statistics, allocator);
    }

    /// <summary>
    /// Builds a decimal array with optimal encoding based on statistics.
    /// </summary>
    public static IArrowArray BuildDecimalArray(
        IReadOnlyList<decimal> values,
        ColumnStatistics? statistics,
        MemoryAllocator allocator,
        double rleThreshold = DefaultRleThreshold)
    {
        return DictionaryArrayBuilder.BuildDecimalArray(values, statistics, allocator);
    }

    /// <summary>
    /// Reads a string value from an array (handles dictionary and primitive).
    /// </summary>
    public static string? GetStringValue(IArrowArray array, int index)
    {
        return DictionaryArrayBuilder.GetStringValue(array, index);
    }

    /// <summary>
    /// Reads an int32 value from an array (handles dictionary and primitive).
    /// </summary>
    public static int GetInt32Value(IArrowArray array, int index)
    {
        return DictionaryArrayBuilder.GetInt32Value(array, index);
    }

    /// <summary>
    /// Reads a double value from an array (handles dictionary and primitive).
    /// </summary>
    public static double GetDoubleValue(IArrowArray array, int index)
    {
        return DictionaryArrayBuilder.GetDoubleValue(array, index);
    }

    /// <summary>
    /// Reads a decimal value from an array (handles dictionary and primitive).
    /// </summary>
    public static decimal GetDecimalValue(IArrowArray array, int index)
    {
        return DictionaryArrayBuilder.GetDecimalValue(array, index);
    }
}
