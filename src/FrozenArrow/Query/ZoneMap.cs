using Apache.Arrow;
using System.Runtime.InteropServices;

namespace FrozenArrow.Query;

/// <summary>
/// Zone maps (also called min-max indices) store the minimum and maximum values
/// for chunks of data, allowing entire chunks to be skipped during predicate evaluation.
/// This is especially powerful for range queries.
/// </summary>
/// <remarks>
/// For example, if a chunk has max(Age) = 45, a query for "Age > 50" can skip the entire chunk
/// without evaluating any rows.
/// </remarks>
public sealed class ZoneMap
{
    /// <summary>
    /// Default chunk size for zone maps (matches parallel chunk size).
    /// </summary>
    public const int DefaultChunkSize = 16_384;

    /// <summary>
    /// Gets the chunk size used for this zone map.
    /// </summary>
    public int ChunkSize { get; }

    /// <summary>
    /// Gets the total number of rows covered by this zone map.
    /// </summary>
    public int TotalRows { get; }

    /// <summary>
    /// Gets the number of chunks in this zone map.
    /// </summary>
    public int ChunkCount => (_totalRows + ChunkSize - 1) / ChunkSize;

    /// <summary>
    /// Per-column zone map data indexed by column name.
    /// </summary>
    private readonly Dictionary<string, ColumnZoneMapData> _columnZoneMaps;

    private readonly int _totalRows;

    public ZoneMap(int totalRows, int chunkSize = DefaultChunkSize)
    {
        if (totalRows < 0)
            throw new ArgumentOutOfRangeException(nameof(totalRows));
        if (chunkSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(chunkSize));

        _totalRows = totalRows;
        ChunkSize = chunkSize;
        TotalRows = totalRows;
        _columnZoneMaps = new Dictionary<string, ColumnZoneMapData>();
    }

    /// <summary>
    /// Adds zone map data for a column.
    /// </summary>
    public void AddColumnZoneMap(string columnName, ColumnZoneMapData data)
    {
        _columnZoneMaps[columnName] = data;
    }

    /// <summary>
    /// Gets zone map data for a column, if available.
    /// </summary>
    public bool TryGetColumnZoneMap(string columnName, out ColumnZoneMapData? data)
    {
        return _columnZoneMaps.TryGetValue(columnName, out data);
    }

    /// <summary>
    /// Checks if a column has zone map data.
    /// </summary>
    public bool HasZoneMapForColumn(string columnName)
    {
        return _columnZoneMaps.ContainsKey(columnName);
    }

    /// <summary>
    /// Builds a zone map from a RecordBatch by scanning the data and computing min/max per chunk.
    /// </summary>
    public static ZoneMap BuildFromRecordBatch(RecordBatch batch, int chunkSize = DefaultChunkSize)
    {
        var zoneMap = new ZoneMap(batch.Length, chunkSize);
        var schema = batch.Schema;

        // Build zone maps for supported column types
        for (int colIdx = 0; colIdx < schema.FieldsList.Count; colIdx++)
        {
            var field = schema.FieldsList[colIdx];
            var column = batch.Column(colIdx);
            var columnName = field.Name;

            var zoneMapData = BuildColumnZoneMap(column, batch.Length, chunkSize);
            if (zoneMapData != null)
            {
                zoneMap.AddColumnZoneMap(columnName, zoneMapData);
            }
        }

        return zoneMap;
    }

    private static ColumnZoneMapData? BuildColumnZoneMap(IArrowArray column, int totalRows, int chunkSize)
    {
        // Build zone maps for supported types
        return column switch
        {
            Int32Array int32Array => BuildInt32ZoneMap(int32Array, totalRows, chunkSize),
            Int64Array int64Array => BuildInt64ZoneMap(int64Array, totalRows, chunkSize),
            DoubleArray doubleArray => BuildDoubleZoneMap(doubleArray, totalRows, chunkSize),
            FloatArray floatArray => BuildFloatZoneMap(floatArray, totalRows, chunkSize),
            Decimal128Array decimalArray => BuildDecimalZoneMap(decimalArray, totalRows, chunkSize),
            // Skip types that don't support range comparisons (strings, bools, etc.)
            _ => null
        };
    }

    private static ColumnZoneMapData BuildInt32ZoneMap(Int32Array array, int totalRows, int chunkSize)
    {
        var chunkCount = (totalRows + chunkSize - 1) / chunkSize;
        var mins = new int[chunkCount];
        var maxs = new int[chunkCount];
        var allNulls = new bool[chunkCount];

        var values = array.Values;

        for (int chunkIdx = 0; chunkIdx < chunkCount; chunkIdx++)
        {
            var startRow = chunkIdx * chunkSize;
            var endRow = Math.Min(startRow + chunkSize, totalRows);

            int min = int.MaxValue;
            int max = int.MinValue;
            bool hasValue = false;

            for (int i = startRow; i < endRow; i++)
            {
                if (!array.IsNull(i))
                {
                    var value = values[i];
                    if (!hasValue)
                    {
                        min = max = value;
                        hasValue = true;
                    }
                    else
                    {
                        if (value < min) min = value;
                        if (value > max) max = value;
                    }
                }
            }

            if (hasValue)
            {
                mins[chunkIdx] = min;
                maxs[chunkIdx] = max;
                allNulls[chunkIdx] = false;
            }
            else
            {
                // Chunk has no non-null values
                allNulls[chunkIdx] = true;
            }
        }

        return new ColumnZoneMapData(
            ZoneMapType.Int32,
            mins.Cast<object>().ToArray(),
            maxs.Cast<object>().ToArray(),
            allNulls);
    }

    private static ColumnZoneMapData BuildInt64ZoneMap(Int64Array array, int totalRows, int chunkSize)
    {
        var chunkCount = (totalRows + chunkSize - 1) / chunkSize;
        var mins = new long[chunkCount];
        var maxs = new long[chunkCount];
        var allNulls = new bool[chunkCount];

        var values = array.Values;

        for (int chunkIdx = 0; chunkIdx < chunkCount; chunkIdx++)
        {
            var startRow = chunkIdx * chunkSize;
            var endRow = Math.Min(startRow + chunkSize, totalRows);

            long min = long.MaxValue;
            long max = long.MinValue;
            bool hasValue = false;

            for (int i = startRow; i < endRow; i++)
            {
                if (!array.IsNull(i))
                {
                    var value = values[i];
                    if (!hasValue)
                    {
                        min = max = value;
                        hasValue = true;
                    }
                    else
                    {
                        if (value < min) min = value;
                        if (value > max) max = value;
                    }
                }
            }

            if (hasValue)
            {
                mins[chunkIdx] = min;
                maxs[chunkIdx] = max;
                allNulls[chunkIdx] = false;
            }
            else
            {
                allNulls[chunkIdx] = true;
            }
        }

        return new ColumnZoneMapData(
            ZoneMapType.Int64,
            mins.Cast<object>().ToArray(),
            maxs.Cast<object>().ToArray(),
            allNulls);
    }

    private static ColumnZoneMapData BuildDoubleZoneMap(DoubleArray array, int totalRows, int chunkSize)
    {
        var chunkCount = (totalRows + chunkSize - 1) / chunkSize;
        var mins = new double[chunkCount];
        var maxs = new double[chunkCount];
        var allNulls = new bool[chunkCount];

        var values = array.Values;

        for (int chunkIdx = 0; chunkIdx < chunkCount; chunkIdx++)
        {
            var startRow = chunkIdx * chunkSize;
            var endRow = Math.Min(startRow + chunkSize, totalRows);

            double min = double.MaxValue;
            double max = double.MinValue;
            bool hasValue = false;

            for (int i = startRow; i < endRow; i++)
            {
                if (!array.IsNull(i))
                {
                    var value = values[i];
                    if (!hasValue)
                    {
                        min = max = value;
                        hasValue = true;
                    }
                    else
                    {
                        if (value < min) min = value;
                        if (value > max) max = value;
                    }
                }
            }

            if (hasValue)
            {
                mins[chunkIdx] = min;
                maxs[chunkIdx] = max;
                allNulls[chunkIdx] = false;
            }
            else
            {
                allNulls[chunkIdx] = true;
            }
        }

        return new ColumnZoneMapData(
            ZoneMapType.Double,
            mins.Cast<object>().ToArray(),
            maxs.Cast<object>().ToArray(),
            allNulls);
    }

    private static ColumnZoneMapData BuildFloatZoneMap(FloatArray array, int totalRows, int chunkSize)
    {
        var chunkCount = (totalRows + chunkSize - 1) / chunkSize;
        var mins = new float[chunkCount];
        var maxs = new float[chunkCount];
        var allNulls = new bool[chunkCount];

        var values = array.Values;

        for (int chunkIdx = 0; chunkIdx < chunkCount; chunkIdx++)
        {
            var startRow = chunkIdx * chunkSize;
            var endRow = Math.Min(startRow + chunkSize, totalRows);

            float min = float.MaxValue;
            float max = float.MinValue;
            bool hasValue = false;

            for (int i = startRow; i < endRow; i++)
            {
                if (!array.IsNull(i))
                {
                    var value = values[i];
                    if (!hasValue)
                    {
                        min = max = value;
                        hasValue = true;
                    }
                    else
                    {
                        if (value < min) min = value;
                        if (value > max) max = value;
                    }
                }
            }

            if (hasValue)
            {
                mins[chunkIdx] = min;
                maxs[chunkIdx] = max;
                allNulls[chunkIdx] = false;
            }
            else
            {
                allNulls[chunkIdx] = true;
            }
        }

        return new ColumnZoneMapData(
            ZoneMapType.Float,
            mins.Cast<object>().ToArray(),
            maxs.Cast<object>().ToArray(),
            allNulls);
    }

    private static ColumnZoneMapData BuildDecimalZoneMap(Decimal128Array array, int totalRows, int chunkSize)
    {
        var chunkCount = (totalRows + chunkSize - 1) / chunkSize;
        var mins = new decimal[chunkCount];
        var maxs = new decimal[chunkCount];
        var allNulls = new bool[chunkCount];

        for (int chunkIdx = 0; chunkIdx < chunkCount; chunkIdx++)
        {
            var startRow = chunkIdx * chunkSize;
            var endRow = Math.Min(startRow + chunkSize, totalRows);

            decimal min = decimal.MaxValue;
            decimal max = decimal.MinValue;
            bool hasValue = false;

            for (int i = startRow; i < endRow; i++)
            {
                if (!array.IsNull(i))
                {
                    var value = array.GetValue(i)!.Value;
                    if (!hasValue)
                    {
                        min = max = value;
                        hasValue = true;
                    }
                    else
                    {
                        if (value < min) min = value;
                        if (value > max) max = value;
                    }
                }
            }

            if (hasValue)
            {
                mins[chunkIdx] = min;
                maxs[chunkIdx] = max;
                allNulls[chunkIdx] = false;
            }
            else
            {
                allNulls[chunkIdx] = true;
            }
        }

        return new ColumnZoneMapData(
            ZoneMapType.Decimal,
            mins.Cast<object>().ToArray(),
            maxs.Cast<object>().ToArray(),
            allNulls);
    }
}


/// <summary>
/// Stores min/max data for a single column across all chunks.
/// Includes pre-computed global min/max for fast selectivity estimation.
/// </summary>
public sealed class ColumnZoneMapData
{
    /// <summary>
    /// The type of values stored in this zone map.
    /// </summary>
    public ZoneMapType Type { get; }

    /// <summary>
    /// Minimum value for each chunk. Array length = chunk count.
    /// </summary>
    public object[] Mins { get; }

    /// <summary>
    /// Maximum value for each chunk. Array length = chunk count.
    /// </summary>
    public object[] Maxs { get; }

    /// <summary>
    /// Indicates whether each chunk contains only null values.
    /// </summary>
    public bool[] AllNulls { get; }
    
    // Pre-computed global min/max for fast selectivity estimation
    private readonly object? _globalMin;
    private readonly object? _globalMax;

    public ColumnZoneMapData(ZoneMapType type, object[] mins, object[] maxs, bool[] allNulls)
    {
        if (mins.Length != maxs.Length || mins.Length != allNulls.Length)
            throw new ArgumentException("Mins, Maxs, and AllNulls arrays must have the same length.");

        Type = type;
        Mins = mins;
        Maxs = maxs;
        AllNulls = allNulls;
        
        // Pre-compute global min/max at construction time (O(chunks), done once)
        (_globalMin, _globalMax) = ComputeGlobalMinMax(type, mins, maxs, allNulls);
    }

    /// <summary>
    /// Gets the number of chunks covered by this zone map.
    /// </summary>
    public int ChunkCount => Mins.Length;
    
    /// <summary>
    /// Gets the pre-computed global min/max for Int32 columns.
    /// Returns (int.MaxValue, int.MinValue) if no non-null values exist.
    /// </summary>
    public (int Min, int Max) GetGlobalMinMaxInt32()
    {
        if (Type != ZoneMapType.Int32 || _globalMin == null || _globalMax == null)
            return (int.MaxValue, int.MinValue);
        return ((int)_globalMin, (int)_globalMax);
    }
    
    /// <summary>
    /// Gets the pre-computed global min/max for Int64 columns.
    /// </summary>
    public (long Min, long Max) GetGlobalMinMaxInt64()
    {
        if (Type != ZoneMapType.Int64 || _globalMin == null || _globalMax == null)
            return (long.MaxValue, long.MinValue);
        return ((long)_globalMin, (long)_globalMax);
    }
    
    /// <summary>
    /// Gets the pre-computed global min/max for Double columns.
    /// </summary>
    public (double Min, double Max) GetGlobalMinMaxDouble()
    {
        if (Type != ZoneMapType.Double || _globalMin == null || _globalMax == null)
            return (double.MaxValue, double.MinValue);
        return ((double)_globalMin, (double)_globalMax);
    }
    
    /// <summary>
    /// Gets the pre-computed global min/max for Float columns.
    /// </summary>
    public (float Min, float Max) GetGlobalMinMaxFloat()
    {
        if (Type != ZoneMapType.Float || _globalMin == null || _globalMax == null)
            return (float.MaxValue, float.MinValue);
        return ((float)_globalMin, (float)_globalMax);
    }
    
    /// <summary>
    /// Gets the pre-computed global min/max for Decimal columns.
    /// </summary>
    public (decimal Min, decimal Max) GetGlobalMinMaxDecimal()
    {
        if (Type != ZoneMapType.Decimal || _globalMin == null || _globalMax == null)
            return (decimal.MaxValue, decimal.MinValue);
        return ((decimal)_globalMin, (decimal)_globalMax);
    }
    
    private static (object? Min, object? Max) ComputeGlobalMinMax(
        ZoneMapType type, object[] mins, object[] maxs, bool[] allNulls)
    {
        return type switch
        {
            ZoneMapType.Int32 => ComputeGlobalMinMaxInt32(mins, maxs, allNulls),
            ZoneMapType.Int64 => ComputeGlobalMinMaxInt64(mins, maxs, allNulls),
            ZoneMapType.Double => ComputeGlobalMinMaxDouble(mins, maxs, allNulls),
            ZoneMapType.Float => ComputeGlobalMinMaxFloat(mins, maxs, allNulls),
            ZoneMapType.Decimal => ComputeGlobalMinMaxDecimal(mins, maxs, allNulls),
            _ => (null, null)
        };
    }
    
    private static (object? Min, object? Max) ComputeGlobalMinMaxInt32(object[] mins, object[] maxs, bool[] allNulls)
    {
        int globalMin = int.MaxValue;
        int globalMax = int.MinValue;
        bool hasValue = false;
        
        for (int i = 0; i < mins.Length; i++)
        {
            if (allNulls[i]) continue;
            hasValue = true;
            var min = (int)mins[i];
            var max = (int)maxs[i];
            if (min < globalMin) globalMin = min;
            if (max > globalMax) globalMax = max;
        }
        
        return hasValue ? (globalMin, globalMax) : (null, null);
    }
    
    private static (object? Min, object? Max) ComputeGlobalMinMaxInt64(object[] mins, object[] maxs, bool[] allNulls)
    {
        long globalMin = long.MaxValue;
        long globalMax = long.MinValue;
        bool hasValue = false;
        
        for (int i = 0; i < mins.Length; i++)
        {
            if (allNulls[i]) continue;
            hasValue = true;
            var min = (long)mins[i];
            var max = (long)maxs[i];
            if (min < globalMin) globalMin = min;
            if (max > globalMax) globalMax = max;
        }
        
        return hasValue ? (globalMin, globalMax) : (null, null);
    }
    
    private static (object? Min, object? Max) ComputeGlobalMinMaxDouble(object[] mins, object[] maxs, bool[] allNulls)
    {
        double globalMin = double.MaxValue;
        double globalMax = double.MinValue;
        bool hasValue = false;
        
        for (int i = 0; i < mins.Length; i++)
        {
            if (allNulls[i]) continue;
            hasValue = true;
            var min = (double)mins[i];
            var max = (double)maxs[i];
            if (min < globalMin) globalMin = min;
            if (max > globalMax) globalMax = max;
        }
        
        return hasValue ? (globalMin, globalMax) : (null, null);
    }
    
    private static (object? Min, object? Max) ComputeGlobalMinMaxFloat(object[] mins, object[] maxs, bool[] allNulls)
    {
        float globalMin = float.MaxValue;
        float globalMax = float.MinValue;
        bool hasValue = false;
        
        for (int i = 0; i < mins.Length; i++)
        {
            if (allNulls[i]) continue;
            hasValue = true;
            var min = (float)mins[i];
            var max = (float)maxs[i];
            if (min < globalMin) globalMin = min;
            if (max > globalMax) globalMax = max;
        }
        
        return hasValue ? (globalMin, globalMax) : (null, null);
    }
    
    private static (object? Min, object? Max) ComputeGlobalMinMaxDecimal(object[] mins, object[] maxs, bool[] allNulls)
    {
        decimal globalMin = decimal.MaxValue;
        decimal globalMax = decimal.MinValue;
        bool hasValue = false;
        
        for (int i = 0; i < mins.Length; i++)
        {
            if (allNulls[i]) continue;
            hasValue = true;
            var min = (decimal)mins[i];
            var max = (decimal)maxs[i];
            if (min < globalMin) globalMin = min;
            if (max > globalMax) globalMax = max;
        }
        
        return hasValue ? (globalMin, globalMax) : (null, null);
    }
}

/// <summary>
/// Types supported by zone maps.
/// </summary>
public enum ZoneMapType
{
    Int32,
    Int64,
    Double,
    Float,
    Decimal
}
