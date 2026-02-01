using Apache.Arrow;

namespace ArrowCollection.Query;

/// <summary>
/// Performs grouped aggregate operations directly on Arrow columns.
/// Groups data by a key column and computes aggregates per group without materializing rows.
/// </summary>
internal static class GroupedColumnAggregator
{
    /// <summary>
    /// Groups indices by key value and returns the grouped indices.
    /// </summary>
    public static Dictionary<TKey, List<int>> GroupIndicesByKey<TKey>(
        IArrowArray keyColumn,
        ref SelectionBitmap selection) where TKey : notnull
    {
        var groups = new Dictionary<TKey, List<int>>();

        foreach (var i in selection.GetSelectedIndices())
        {
            if (keyColumn.IsNull(i))
                continue; // Skip nulls in group key

            var key = GetValue<TKey>(keyColumn, i);
            if (!groups.TryGetValue(key, out var list))
            {
                list = [];
                groups[key] = list;
            }
            list.Add(i);
        }

        return groups;
    }

    /// <summary>
    /// Computes Sum for each group.
    /// </summary>
    public static Dictionary<TKey, TResult> SumByGroup<TKey, TResult>(
        IArrowArray keyColumn,
        IArrowArray valueColumn,
        ref SelectionBitmap selection) where TKey : notnull
    {
        var groups = GroupIndicesByKey<TKey>(keyColumn, ref selection);
        var results = new Dictionary<TKey, TResult>();

        foreach (var (key, indices) in groups)
        {
            var sum = SumIndices(valueColumn, indices);
            results[key] = (TResult)Convert.ChangeType(sum, typeof(TResult));
        }

        return results;
    }

    /// <summary>
    /// Computes Count for each group.
    /// </summary>
    public static Dictionary<TKey, int> CountByGroup<TKey>(
        IArrowArray keyColumn,
        ref SelectionBitmap selection) where TKey : notnull
    {
        var groups = GroupIndicesByKey<TKey>(keyColumn, ref selection);
        var results = new Dictionary<TKey, int>();

        foreach (var (key, indices) in groups)
        {
            results[key] = indices.Count;
        }

        return results;
    }

    /// <summary>
    /// Computes Average for each group.
    /// </summary>
    public static Dictionary<TKey, double> AverageByGroup<TKey>(
        IArrowArray keyColumn,
        IArrowArray valueColumn,
        ref SelectionBitmap selection) where TKey : notnull
    {
        var groups = GroupIndicesByKey<TKey>(keyColumn, ref selection);
        var results = new Dictionary<TKey, double>();

        foreach (var (key, indices) in groups)
        {
            var sum = SumIndices(valueColumn, indices);
            results[key] = indices.Count > 0 ? sum / indices.Count : 0;
        }

        return results;
    }

    /// <summary>
    /// Computes Min for each group.
    /// </summary>
    public static Dictionary<TKey, TResult> MinByGroup<TKey, TResult>(
        IArrowArray keyColumn,
        IArrowArray valueColumn,
        ref SelectionBitmap selection) 
        where TKey : notnull
        where TResult : IComparable<TResult>
    {
        var groups = GroupIndicesByKey<TKey>(keyColumn, ref selection);
        var results = new Dictionary<TKey, TResult>();

        foreach (var (key, indices) in groups)
        {
            if (indices.Count == 0) continue;
            var min = MinIndices<TResult>(valueColumn, indices);
            results[key] = min;
        }

        return results;
    }

    /// <summary>
    /// Computes Max for each group.
    /// </summary>
    public static Dictionary<TKey, TResult> MaxByGroup<TKey, TResult>(
        IArrowArray keyColumn,
        IArrowArray valueColumn,
        ref SelectionBitmap selection)
        where TKey : notnull
        where TResult : IComparable<TResult>
    {
        var groups = GroupIndicesByKey<TKey>(keyColumn, ref selection);
        var results = new Dictionary<TKey, TResult>();

        foreach (var (key, indices) in groups)
        {
            if (indices.Count == 0) continue;
            var max = MaxIndices<TResult>(valueColumn, indices);
            results[key] = max;
        }

        return results;
    }

    /// <summary>
    /// Executes a full grouped query with multiple aggregates.
    /// Returns a list of result objects with Key and aggregate values.
    /// </summary>
    public static List<GroupedResult<TKey>> ExecuteGroupedQuery<TKey>(
        IArrowArray keyColumn,
        RecordBatch batch,
        ref SelectionBitmap selection,
        IReadOnlyList<AggregationDescriptor> aggregations,
        Dictionary<string, int> columnIndexMap) where TKey : notnull
    {
        var groups = GroupIndicesByKey<TKey>(keyColumn, ref selection);
        var results = new List<GroupedResult<TKey>>(groups.Count);

        foreach (var (key, indices) in groups)
        {
            var result = new GroupedResult<TKey> { Key = key };

            foreach (var agg in aggregations)
            {
                object value = agg.Operation switch
                {
                    AggregationOperation.Count => indices.Count,
                    AggregationOperation.LongCount => (long)indices.Count,
                    AggregationOperation.Sum => ComputeSum(batch, columnIndexMap, agg.ColumnName!, indices),
                    AggregationOperation.Average => ComputeAverage(batch, columnIndexMap, agg.ColumnName!, indices),
                    AggregationOperation.Min => ComputeMin(batch, columnIndexMap, agg.ColumnName!, indices),
                    AggregationOperation.Max => ComputeMax(batch, columnIndexMap, agg.ColumnName!, indices),
                    _ => throw new NotSupportedException($"Aggregation {agg.Operation} is not supported.")
                };

                result.AggregateValues[agg.ResultPropertyName] = value;
            }

            results.Add(result);
        }

        return results;
    }

    #region Helper Methods

    private static T GetValue<T>(IArrowArray column, int index)
    {
        return column switch
        {
            StringArray stringArray => (T)(object)stringArray.GetString(index)!,
            Int32Array int32Array => (T)(object)int32Array.Values[index],
            Int64Array int64Array => (T)(object)int64Array.Values[index],
            DoubleArray doubleArray => (T)(object)doubleArray.Values[index],
            BooleanArray boolArray => (T)(object)boolArray.GetValue(index)!,
            // Handle dictionary-encoded strings
            DictionaryArray dictArray when dictArray.Dictionary is StringArray dictStrings =>
                (T)(object)dictStrings.GetString(GetDictionaryIndex(dictArray, index))!,
            _ => throw new NotSupportedException($"Column type {column.GetType().Name} is not supported for grouping.")
        };
    }

    private static int GetDictionaryIndex(DictionaryArray dictArray, int index)
    {
        return dictArray.Indices switch
        {
            Int8Array i8 => i8.Values[index],
            Int16Array i16 => i16.Values[index],
            Int32Array i32 => i32.Values[index],
            Int64Array i64 => (int)i64.Values[index],
            _ => throw new NotSupportedException("Unsupported dictionary index type.")
        };
    }

    private static double SumIndices(IArrowArray column, List<int> indices)
    {
        double sum = 0;
        foreach (var i in indices)
        {
            if (column.IsNull(i)) continue;
            sum += column switch
            {
                Int32Array int32Array => int32Array.Values[i],
                Int64Array int64Array => int64Array.Values[i],
                DoubleArray doubleArray => doubleArray.Values[i],
                FloatArray floatArray => floatArray.Values[i],
                Decimal128Array decimalArray => (double)decimalArray.GetValue(i)!.Value,
                _ => throw new NotSupportedException($"Sum not supported for {column.GetType().Name}")
            };
        }
        return sum;
    }

    private static T MinIndices<T>(IArrowArray column, List<int> indices) where T : IComparable<T>
    {
        T? min = default;
        bool hasValue = false;

        foreach (var i in indices)
        {
            if (column.IsNull(i)) continue;
            var value = GetNumericValue<T>(column, i);
            if (!hasValue || value.CompareTo(min!) < 0)
            {
                min = value;
                hasValue = true;
            }
        }

        if (!hasValue)
            throw new InvalidOperationException("Sequence contains no elements.");

        return min!;
    }

    private static T MaxIndices<T>(IArrowArray column, List<int> indices) where T : IComparable<T>
    {
        T? max = default;
        bool hasValue = false;

        foreach (var i in indices)
        {
            if (column.IsNull(i)) continue;
            var value = GetNumericValue<T>(column, i);
            if (!hasValue || value.CompareTo(max!) > 0)
            {
                max = value;
                hasValue = true;
            }
        }

        if (!hasValue)
            throw new InvalidOperationException("Sequence contains no elements.");

        return max!;
    }

    private static T GetNumericValue<T>(IArrowArray column, int index)
    {
        object value = column switch
        {
            Int32Array int32Array => int32Array.Values[index],
            Int64Array int64Array => int64Array.Values[index],
            DoubleArray doubleArray => doubleArray.Values[index],
            FloatArray floatArray => floatArray.Values[index],
            Decimal128Array decimalArray => decimalArray.GetValue(index)!.Value,
            _ => throw new NotSupportedException($"Numeric access not supported for {column.GetType().Name}")
        };
        return (T)Convert.ChangeType(value, typeof(T));
    }

    private static object ComputeSum(RecordBatch batch, Dictionary<string, int> columnIndexMap, string columnName, List<int> indices)
    {
        var column = batch.Column(columnIndexMap[columnName]);
        return column switch
        {
            Int32Array => (int)SumIndices(column, indices),
            Int64Array => (long)SumIndices(column, indices),
            DoubleArray => SumIndices(column, indices),
            FloatArray => (float)SumIndices(column, indices),
            Decimal128Array => SumDecimalIndices((Decimal128Array)column, indices),
            _ => throw new NotSupportedException($"Sum not supported for {column.GetType().Name}")
        };
    }

    private static decimal SumDecimalIndices(Decimal128Array column, List<int> indices)
    {
        decimal sum = 0;
        foreach (var i in indices)
        {
            if (!column.IsNull(i))
                sum += column.GetValue(i)!.Value;
        }
        return sum;
    }

    private static double ComputeAverage(RecordBatch batch, Dictionary<string, int> columnIndexMap, string columnName, List<int> indices)
    {
        var column = batch.Column(columnIndexMap[columnName]);
        var sum = SumIndices(column, indices);
        var count = indices.Count(i => !column.IsNull(i));
        return count > 0 ? sum / count : 0.0;
    }

    private static object ComputeMin(RecordBatch batch, Dictionary<string, int> columnIndexMap, string columnName, List<int> indices)
    {
        var column = batch.Column(columnIndexMap[columnName]);
        return column switch
        {
            Int32Array => MinIndices<int>(column, indices),
            Int64Array => MinIndices<long>(column, indices),
            DoubleArray => MinIndices<double>(column, indices),
            Decimal128Array => MinDecimalIndices((Decimal128Array)column, indices),
            _ => throw new NotSupportedException($"Min not supported for {column.GetType().Name}")
        };
    }

    private static decimal MinDecimalIndices(Decimal128Array column, List<int> indices)
    {
        decimal min = decimal.MaxValue;
        bool hasValue = false;
        foreach (var i in indices)
        {
            if (!column.IsNull(i))
            {
                var value = column.GetValue(i)!.Value;
                if (!hasValue || value < min)
                {
                    min = value;
                    hasValue = true;
                }
            }
        }
        if (!hasValue) throw new InvalidOperationException("Sequence contains no elements.");
        return min;
    }

    private static object ComputeMax(RecordBatch batch, Dictionary<string, int> columnIndexMap, string columnName, List<int> indices)
    {
        var column = batch.Column(columnIndexMap[columnName]);
        return column switch
        {
            Int32Array => MaxIndices<int>(column, indices),
            Int64Array => MaxIndices<long>(column, indices),
            DoubleArray => MaxIndices<double>(column, indices),
            Decimal128Array => MaxDecimalIndices((Decimal128Array)column, indices),
            _ => throw new NotSupportedException($"Max not supported for {column.GetType().Name}")
        };
    }

    private static decimal MaxDecimalIndices(Decimal128Array column, List<int> indices)
    {
        decimal max = decimal.MinValue;
        bool hasValue = false;
        foreach (var i in indices)
        {
            if (!column.IsNull(i))
            {
                var value = column.GetValue(i)!.Value;
                if (!hasValue || value > max)
                {
                    max = value;
                    hasValue = true;
                }
            }
        }
        if (!hasValue) throw new InvalidOperationException("Sequence contains no elements.");
        return max;
    }

    #endregion
}

/// <summary>
/// Represents a grouped result with a key and aggregate values.
/// </summary>
public sealed class GroupedResult<TKey>
{
    /// <summary>
    /// The group key.
    /// </summary>
    public TKey Key { get; set; } = default!;

    /// <summary>
    /// The computed aggregate values, keyed by property name.
    /// </summary>
    public Dictionary<string, object> AggregateValues { get; } = [];
}
