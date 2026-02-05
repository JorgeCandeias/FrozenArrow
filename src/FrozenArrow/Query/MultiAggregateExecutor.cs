using Apache.Arrow;

namespace FrozenArrow.Query;

/// <summary>
/// Executes multiple aggregate operations in a single pass over the data.
/// </summary>
internal static class MultiAggregateExecutor
{
    /// <summary>
    /// Computes multiple aggregates over the selected rows and returns the results.
    /// </summary>
    public static Dictionary<string, object> Execute(
        RecordBatch batch,
        ref SelectionBitmap selection,
        IReadOnlyList<AggregationDescriptor> aggregations,
        Dictionary<string, int> columnIndexMap)
    {
        var results = new Dictionary<string, object>(aggregations.Count);

        // For Count operations, we just need the selection count
        var selectionCount = -1; // Lazy compute

        foreach (var agg in aggregations)
        {
            object value = agg.Operation switch
            {
                AggregationOperation.Count => selectionCount < 0 
                    ? (selectionCount = selection.CountSet()) 
                    : selectionCount,
                AggregationOperation.LongCount => (long)(selectionCount < 0 
                    ? (selectionCount = selection.CountSet()) 
                    : selectionCount),
                AggregationOperation.Sum => ComputeSum(batch, columnIndexMap, agg.ColumnName!, ref selection),
                AggregationOperation.Average => ComputeAverage(batch, columnIndexMap, agg.ColumnName!, ref selection),
                AggregationOperation.Min => ComputeMin(batch, columnIndexMap, agg.ColumnName!, ref selection),
                AggregationOperation.Max => ComputeMax(batch, columnIndexMap, agg.ColumnName!, ref selection),
                _ => throw new NotSupportedException($"Aggregation {agg.Operation} is not supported.")
            };

            results[agg.ResultPropertyName] = value;
        }

        return results;
    }

    private static object ComputeSum(RecordBatch batch, Dictionary<string, int> columnIndexMap, string columnName, ref SelectionBitmap selection)
    {
        var column = batch.Column(columnIndexMap[columnName]);
        return column switch
        {
            Int32Array int32Array => SumInt32(int32Array, ref selection),
            Int64Array int64Array => SumInt64(int64Array, ref selection),
            DoubleArray doubleArray => SumDouble(doubleArray, ref selection),
            FloatArray floatArray => (float)SumFloat(floatArray, ref selection),
            Decimal128Array decimalArray => SumDecimal(decimalArray, ref selection),
            DictionaryArray dictArray => SumDictionary(dictArray, ref selection),
            _ => throw new NotSupportedException($"Sum not supported for {column.GetType().Name}")
        };
    }

    private static object ComputeAverage(RecordBatch batch, Dictionary<string, int> columnIndexMap, string columnName, ref SelectionBitmap selection)
    {
        var column = batch.Column(columnIndexMap[columnName]);
        return column switch
        {
            Int32Array int32Array => AverageInt32(int32Array, ref selection),
            Int64Array int64Array => AverageInt64(int64Array, ref selection),
            DoubleArray doubleArray => AverageDouble(doubleArray, ref selection),
            FloatArray floatArray => AverageFloat(floatArray, ref selection),
            Decimal128Array decimalArray => (double)AverageDecimal(decimalArray, ref selection),
            DictionaryArray dictArray => AverageDictionary(dictArray, ref selection),
            _ => throw new NotSupportedException($"Average not supported for {column.GetType().Name}")
        };
    }

    private static object ComputeMin(RecordBatch batch, Dictionary<string, int> columnIndexMap, string columnName, ref SelectionBitmap selection)
    {
        var column = batch.Column(columnIndexMap[columnName]);
        return column switch
        {
            Int32Array int32Array => MinInt32(int32Array, ref selection),
            Int64Array int64Array => MinInt64(int64Array, ref selection),
            DoubleArray doubleArray => MinDouble(doubleArray, ref selection),
            Decimal128Array decimalArray => MinDecimal(decimalArray, ref selection),
            DictionaryArray dictArray => MinDictionary(dictArray, ref selection),
            _ => throw new NotSupportedException($"Min not supported for {column.GetType().Name}")
        };
    }

    private static object ComputeMax(RecordBatch batch, Dictionary<string, int> columnIndexMap, string columnName, ref SelectionBitmap selection)
    {
        var column = batch.Column(columnIndexMap[columnName]);
        return column switch
        {
            Int32Array int32Array => MaxInt32(int32Array, ref selection),
            Int64Array int64Array => MaxInt64(int64Array, ref selection),
            DoubleArray doubleArray => MaxDouble(doubleArray, ref selection),
            Decimal128Array decimalArray => MaxDecimal(decimalArray, ref selection),
            DictionaryArray dictArray => MaxDictionary(dictArray, ref selection),
            _ => throw new NotSupportedException($"Max not supported for {column.GetType().Name}")
        };
    }

    #region Aggregate Implementations

    private static int SumInt32(Int32Array array, ref SelectionBitmap selection)
    {
        long sum = 0;
        var span = array.Values;
        foreach (var i in selection.GetSelectedIndices())
        {
            if (!array.IsNull(i))
                sum += span[i];
        }
        return (int)sum;
    }

    private static long SumInt64(Int64Array array, ref SelectionBitmap selection)
    {
        long sum = 0;
        var span = array.Values;
        foreach (var i in selection.GetSelectedIndices())
        {
            if (!array.IsNull(i))
                sum += span[i];
        }
        return sum;
    }

    private static double SumDouble(DoubleArray array, ref SelectionBitmap selection)
    {
        double sum = 0;
        var span = array.Values;
        foreach (var i in selection.GetSelectedIndices())
        {
            if (!array.IsNull(i))
                sum += span[i];
        }
        return sum;
    }

    private static double SumFloat(FloatArray array, ref SelectionBitmap selection)
    {
        double sum = 0;
        var span = array.Values;
        foreach (var i in selection.GetSelectedIndices())
        {
            if (!array.IsNull(i))
                sum += span[i];
        }
        return sum;
    }

    private static decimal SumDecimal(Decimal128Array array, ref SelectionBitmap selection)
    {
        decimal sum = 0;
        foreach (var i in selection.GetSelectedIndices())
        {
            if (!array.IsNull(i))
                sum += array.GetValue(i)!.Value;
        }
        return sum;
    }

    private static double AverageInt32(Int32Array array, ref SelectionBitmap selection)
    {
        long sum = 0;
        int count = 0;
        var span = array.Values;
        foreach (var i in selection.GetSelectedIndices())
        {
            if (!array.IsNull(i))
            {
                sum += span[i];
                count++;
            }
        }
        return count > 0 ? (double)sum / count : throw new InvalidOperationException("Sequence contains no elements");
    }

    private static double AverageInt64(Int64Array array, ref SelectionBitmap selection)
    {
        long sum = 0;
        int count = 0;
        var span = array.Values;
        foreach (var i in selection.GetSelectedIndices())
        {
            if (!array.IsNull(i))
            {
                sum += span[i];
                count++;
            }
        }
        return count > 0 ? (double)sum / count : throw new InvalidOperationException("Sequence contains no elements");
    }

    private static double AverageDouble(DoubleArray array, ref SelectionBitmap selection)
    {
        double sum = 0;
        int count = 0;
        var span = array.Values;
        foreach (var i in selection.GetSelectedIndices())
        {
            if (!array.IsNull(i))
            {
                sum += span[i];
                count++;
            }
        }
        return count > 0 ? sum / count : throw new InvalidOperationException("Sequence contains no elements");
    }

    private static double AverageFloat(FloatArray array, ref SelectionBitmap selection)
    {
        double sum = 0;
        int count = 0;
        var span = array.Values;
        foreach (var i in selection.GetSelectedIndices())
        {
            if (!array.IsNull(i))
            {
                sum += span[i];
                count++;
            }
        }
        return count > 0 ? sum / count : throw new InvalidOperationException("Sequence contains no elements");
    }

    private static decimal AverageDecimal(Decimal128Array array, ref SelectionBitmap selection)
    {
        decimal sum = 0;
        int count = 0;
        foreach (var i in selection.GetSelectedIndices())
        {
            if (!array.IsNull(i))
            {
                sum += array.GetValue(i)!.Value;
                count++;
            }
        }
        return count > 0 ? sum / count : throw new InvalidOperationException("Sequence contains no elements");
    }

    private static int MinInt32(Int32Array array, ref SelectionBitmap selection)
    {
        int min = int.MaxValue;
        bool hasValue = false;
        var span = array.Values;
        foreach (var i in selection.GetSelectedIndices())
        {
            if (!array.IsNull(i))
            {
                var value = span[i];
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

    private static long MinInt64(Int64Array array, ref SelectionBitmap selection)
    {
        long min = long.MaxValue;
        bool hasValue = false;
        var span = array.Values;
        foreach (var i in selection.GetSelectedIndices())
        {
            if (!array.IsNull(i))
            {
                var value = span[i];
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

    private static double MinDouble(DoubleArray array, ref SelectionBitmap selection)
    {
        double min = double.MaxValue;
        bool hasValue = false;
        var span = array.Values;
        foreach (var i in selection.GetSelectedIndices())
        {
            if (!array.IsNull(i))
            {
                var value = span[i];
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

    private static decimal MinDecimal(Decimal128Array array, ref SelectionBitmap selection)
    {
        decimal min = decimal.MaxValue;
        bool hasValue = false;
        foreach (var i in selection.GetSelectedIndices())
        {
            if (!array.IsNull(i))
            {
                var value = array.GetValue(i)!.Value;
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

    private static int MaxInt32(Int32Array array, ref SelectionBitmap selection)
    {
        int max = int.MinValue;
        bool hasValue = false;
        var span = array.Values;
        foreach (var i in selection.GetSelectedIndices())
        {
            if (!array.IsNull(i))
            {
                var value = span[i];
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

    private static long MaxInt64(Int64Array array, ref SelectionBitmap selection)
    {
        long max = long.MinValue;
        bool hasValue = false;
        var span = array.Values;
        foreach (var i in selection.GetSelectedIndices())
        {
            if (!array.IsNull(i))
            {
                var value = span[i];
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

    private static double MaxDouble(DoubleArray array, ref SelectionBitmap selection)
    {
        double max = double.MinValue;
        bool hasValue = false;
        var span = array.Values;
        foreach (var i in selection.GetSelectedIndices())
        {
            if (!array.IsNull(i))
            {
                var value = span[i];
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

    private static decimal MaxDecimal(Decimal128Array array, ref SelectionBitmap selection)
    {
        decimal max = decimal.MinValue;
        bool hasValue = false;
        foreach (var i in selection.GetSelectedIndices())
        {
            if (!array.IsNull(i))
            {
                var value = array.GetValue(i)!.Value;
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

    #region DictionaryArray Support

    private static object SumDictionary(DictionaryArray dictArray, ref SelectionBitmap selection)
    {
        if (dictArray.Dictionary is Decimal128Array)
        {
            decimal sum = 0;
            foreach (var i in selection.GetSelectedIndices())
            {
                if (!dictArray.IsNull(i))
                    sum += DictionaryArrayHelper.GetDecimalValue(dictArray, i);
            }
            return sum;
        }
        else
        {
            double sum = 0;
            foreach (var i in selection.GetSelectedIndices())
            {
                if (!dictArray.IsNull(i))
                    sum += DictionaryArrayHelper.GetNumericValue(dictArray, i);
            }
            return sum;
        }
    }

    private static double AverageDictionary(DictionaryArray dictArray, ref SelectionBitmap selection)
    {
        double sum = 0;
        int count = 0;
        foreach (var i in selection.GetSelectedIndices())
        {
            if (!dictArray.IsNull(i))
            {
                sum += DictionaryArrayHelper.GetNumericValue(dictArray, i);
                count++;
            }
        }
        return count > 0 ? sum / count : throw new InvalidOperationException("Sequence contains no elements");
    }

    private static object MinDictionary(DictionaryArray dictArray, ref SelectionBitmap selection)
    {
        if (dictArray.Dictionary is Decimal128Array)
        {
            decimal min = decimal.MaxValue;
            bool hasValue = false;
            foreach (var i in selection.GetSelectedIndices())
            {
                if (!dictArray.IsNull(i))
                {
                    var value = DictionaryArrayHelper.GetDecimalValue(dictArray, i);
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
        else
        {
            double min = double.MaxValue;
            bool hasValue = false;
            foreach (var i in selection.GetSelectedIndices())
            {
                if (!dictArray.IsNull(i))
                {
                    var value = DictionaryArrayHelper.GetNumericValue(dictArray, i);
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
    }

    private static object MaxDictionary(DictionaryArray dictArray, ref SelectionBitmap selection)
    {
        if (dictArray.Dictionary is Decimal128Array)
        {
            decimal max = decimal.MinValue;
            bool hasValue = false;
            foreach (var i in selection.GetSelectedIndices())
            {
                if (!dictArray.IsNull(i))
                {
                    var value = DictionaryArrayHelper.GetDecimalValue(dictArray, i);
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
        else
        {
            double max = double.MinValue;
            bool hasValue = false;
            foreach (var i in selection.GetSelectedIndices())
            {
                if (!dictArray.IsNull(i))
                {
                    var value = DictionaryArrayHelper.GetNumericValue(dictArray, i);
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
    }

    #endregion
}

