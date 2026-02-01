using Apache.Arrow;

namespace ArrowCollection.Query;

/// <summary>
/// Performs aggregate operations directly on Arrow columns without materializing rows.
/// </summary>
internal static class ColumnAggregator
{
    #region Sum Operations

    public static long SumInt32(Int32Array array, bool[] selection)
    {
        long sum = 0;
        var span = array.Values;
        for (int i = 0; i < array.Length; i++)
        {
            if (selection[i] && !array.IsNull(i))
            {
                sum += span[i];
            }
        }
        return sum;
    }

    public static long SumInt64(Int64Array array, bool[] selection)
    {
        long sum = 0;
        var span = array.Values;
        for (int i = 0; i < array.Length; i++)
        {
            if (selection[i] && !array.IsNull(i))
            {
                sum += span[i];
            }
        }
        return sum;
    }

    public static double SumDouble(DoubleArray array, bool[] selection)
    {
        double sum = 0;
        var span = array.Values;
        for (int i = 0; i < array.Length; i++)
        {
            if (selection[i] && !array.IsNull(i))
            {
                sum += span[i];
            }
        }
        return sum;
    }

    public static double SumFloat(FloatArray array, bool[] selection)
    {
        double sum = 0;
        var span = array.Values;
        for (int i = 0; i < array.Length; i++)
        {
            if (selection[i] && !array.IsNull(i))
            {
                sum += span[i];
            }
        }
        return sum;
    }

    public static decimal SumDecimal(Decimal128Array array, bool[] selection)
    {
        decimal sum = 0;
        for (int i = 0; i < array.Length; i++)
        {
            if (selection[i] && !array.IsNull(i))
            {
                sum += (decimal)array.GetSqlDecimal(i);
            }
        }
        return sum;
    }

    #endregion

    #region Average Operations

    public static double AverageInt32(Int32Array array, bool[] selection)
    {
        long sum = 0;
        int count = 0;
        var span = array.Values;
        for (int i = 0; i < array.Length; i++)
        {
            if (selection[i] && !array.IsNull(i))
            {
                sum += span[i];
                count++;
            }
        }
        return count > 0 ? (double)sum / count : 0;
    }

    public static double AverageInt64(Int64Array array, bool[] selection)
    {
        long sum = 0;
        int count = 0;
        var span = array.Values;
        for (int i = 0; i < array.Length; i++)
        {
            if (selection[i] && !array.IsNull(i))
            {
                sum += span[i];
                count++;
            }
        }
        return count > 0 ? (double)sum / count : 0;
    }

    public static double AverageDouble(DoubleArray array, bool[] selection)
    {
        double sum = 0;
        int count = 0;
        var span = array.Values;
        for (int i = 0; i < array.Length; i++)
        {
            if (selection[i] && !array.IsNull(i))
            {
                sum += span[i];
                count++;
            }
        }
        return count > 0 ? sum / count : 0;
    }

    public static double AverageFloat(FloatArray array, bool[] selection)
    {
        double sum = 0;
        int count = 0;
        var span = array.Values;
        for (int i = 0; i < array.Length; i++)
        {
            if (selection[i] && !array.IsNull(i))
            {
                sum += span[i];
                count++;
            }
        }
        return count > 0 ? sum / count : 0;
    }

    public static decimal AverageDecimal(Decimal128Array array, bool[] selection)
    {
        decimal sum = 0;
        int count = 0;
        for (int i = 0; i < array.Length; i++)
        {
            if (selection[i] && !array.IsNull(i))
            {
                sum += (decimal)array.GetSqlDecimal(i);
                count++;
            }
        }
        return count > 0 ? sum / count : 0;
    }

    #endregion

    #region Min Operations

    public static int MinInt32(Int32Array array, bool[] selection)
    {
        int min = int.MaxValue;
        bool hasValue = false;
        var span = array.Values;
        for (int i = 0; i < array.Length; i++)
        {
            if (selection[i] && !array.IsNull(i))
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

    public static long MinInt64(Int64Array array, bool[] selection)
    {
        long min = long.MaxValue;
        bool hasValue = false;
        var span = array.Values;
        for (int i = 0; i < array.Length; i++)
        {
            if (selection[i] && !array.IsNull(i))
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

    public static double MinDouble(DoubleArray array, bool[] selection)
    {
        double min = double.MaxValue;
        bool hasValue = false;
        var span = array.Values;
        for (int i = 0; i < array.Length; i++)
        {
            if (selection[i] && !array.IsNull(i))
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

    public static decimal MinDecimal(Decimal128Array array, bool[] selection)
    {
        decimal min = decimal.MaxValue;
        bool hasValue = false;
        for (int i = 0; i < array.Length; i++)
        {
            if (selection[i] && !array.IsNull(i))
            {
                var value = (decimal)array.GetSqlDecimal(i);
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

    public static DateTime MinDateTime(TimestampArray array, bool[] selection)
    {
        DateTimeOffset min = DateTimeOffset.MaxValue;
        bool hasValue = false;
        for (int i = 0; i < array.Length; i++)
        {
            if (selection[i] && !array.IsNull(i))
            {
                var value = array.GetTimestamp(i)!.Value;
                if (!hasValue || value < min)
                {
                    min = value;
                    hasValue = true;
                }
            }
        }
        if (!hasValue) throw new InvalidOperationException("Sequence contains no elements.");
        return min.UtcDateTime;
    }

    #endregion

    #region Max Operations

    public static int MaxInt32(Int32Array array, bool[] selection)
    {
        int max = int.MinValue;
        bool hasValue = false;
        var span = array.Values;
        for (int i = 0; i < array.Length; i++)
        {
            if (selection[i] && !array.IsNull(i))
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

    public static long MaxInt64(Int64Array array, bool[] selection)
    {
        long max = long.MinValue;
        bool hasValue = false;
        var span = array.Values;
        for (int i = 0; i < array.Length; i++)
        {
            if (selection[i] && !array.IsNull(i))
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

    public static double MaxDouble(DoubleArray array, bool[] selection)
    {
        double max = double.MinValue;
        bool hasValue = false;
        var span = array.Values;
        for (int i = 0; i < array.Length; i++)
        {
            if (selection[i] && !array.IsNull(i))
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

    public static decimal MaxDecimal(Decimal128Array array, bool[] selection)
    {
        decimal max = decimal.MinValue;
        bool hasValue = false;
        for (int i = 0; i < array.Length; i++)
        {
            if (selection[i] && !array.IsNull(i))
            {
                var value = (decimal)array.GetSqlDecimal(i);
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

    public static DateTime MaxDateTime(TimestampArray array, bool[] selection)
    {
        DateTimeOffset max = DateTimeOffset.MinValue;
        bool hasValue = false;
        for (int i = 0; i < array.Length; i++)
        {
            if (selection[i] && !array.IsNull(i))
            {
                var value = array.GetTimestamp(i)!.Value;
                if (!hasValue || value > max)
                {
                    max = value;
                    hasValue = true;
                }
            }
        }
        if (!hasValue) throw new InvalidOperationException("Sequence contains no elements.");
        return max.UtcDateTime;
    }

    #endregion

    #region Dispatch Methods

    /// <summary>
    /// Executes a Sum operation on the specified column using the selection bitmap.
    /// </summary>
    public static object ExecuteSum(IArrowArray column, bool[] selection, Type resultType)
    {
        return column switch
        {
            Int32Array int32Array => ConvertResult(SumInt32(int32Array, selection), resultType),
            Int64Array int64Array => ConvertResult(SumInt64(int64Array, selection), resultType),
            DoubleArray doubleArray => ConvertResult(SumDouble(doubleArray, selection), resultType),
            FloatArray floatArray => ConvertResult(SumFloat(floatArray, selection), resultType),
            Decimal128Array decimalArray => ConvertResult(SumDecimal(decimalArray, selection), resultType),
            _ => throw new NotSupportedException($"Sum is not supported for column type {column.GetType().Name}")
        };
    }

    /// <summary>
    /// Executes an Average operation on the specified column using the selection bitmap.
    /// </summary>
    public static object ExecuteAverage(IArrowArray column, bool[] selection, Type resultType)
    {
        return column switch
        {
            Int32Array int32Array => ConvertResult(AverageInt32(int32Array, selection), resultType),
            Int64Array int64Array => ConvertResult(AverageInt64(int64Array, selection), resultType),
            DoubleArray doubleArray => ConvertResult(AverageDouble(doubleArray, selection), resultType),
            FloatArray floatArray => ConvertResult(AverageFloat(floatArray, selection), resultType),
            Decimal128Array decimalArray => ConvertResult(AverageDecimal(decimalArray, selection), resultType),
            _ => throw new NotSupportedException($"Average is not supported for column type {column.GetType().Name}")
        };
    }

    /// <summary>
    /// Executes a Min operation on the specified column using the selection bitmap.
    /// </summary>
    public static object ExecuteMin(IArrowArray column, bool[] selection, Type resultType)
    {
        return column switch
        {
            Int32Array int32Array => ConvertResult(MinInt32(int32Array, selection), resultType),
            Int64Array int64Array => ConvertResult(MinInt64(int64Array, selection), resultType),
            DoubleArray doubleArray => ConvertResult(MinDouble(doubleArray, selection), resultType),
            Decimal128Array decimalArray => ConvertResult(MinDecimal(decimalArray, selection), resultType),
            TimestampArray timestampArray => MinDateTime(timestampArray, selection),
            _ => throw new NotSupportedException($"Min is not supported for column type {column.GetType().Name}")
        };
    }

    /// <summary>
    /// Executes a Max operation on the specified column using the selection bitmap.
    /// </summary>
    public static object ExecuteMax(IArrowArray column, bool[] selection, Type resultType)
    {
        return column switch
        {
            Int32Array int32Array => ConvertResult(MaxInt32(int32Array, selection), resultType),
            Int64Array int64Array => ConvertResult(MaxInt64(int64Array, selection), resultType),
            DoubleArray doubleArray => ConvertResult(MaxDouble(doubleArray, selection), resultType),
            Decimal128Array decimalArray => ConvertResult(MaxDecimal(decimalArray, selection), resultType),
            TimestampArray timestampArray => MaxDateTime(timestampArray, selection),
            _ => throw new NotSupportedException($"Max is not supported for column type {column.GetType().Name}")
        };
    }

    private static object ConvertResult(object value, Type targetType)
    {
        if (targetType == typeof(int)) return Convert.ToInt32(value);
        if (targetType == typeof(long)) return Convert.ToInt64(value);
        if (targetType == typeof(double)) return Convert.ToDouble(value);
        if (targetType == typeof(float)) return Convert.ToSingle(value);
        if (targetType == typeof(decimal)) return Convert.ToDecimal(value);
        return value;
    }

    #endregion

    #region SelectionBitmap Overloads

    /// <summary>
    /// Executes a Sum operation using the compact SelectionBitmap.
    /// </summary>
    public static object ExecuteSum(IArrowArray column, ref SelectionBitmap selection, Type resultType)
    {
        return column switch
        {
            Int32Array int32Array => ConvertResult(SumInt32(int32Array, ref selection), resultType),
            Int64Array int64Array => ConvertResult(SumInt64(int64Array, ref selection), resultType),
            DoubleArray doubleArray => ConvertResult(SumDouble(doubleArray, ref selection), resultType),
            FloatArray floatArray => ConvertResult(SumFloat(floatArray, ref selection), resultType),
            Decimal128Array decimalArray => ConvertResult(SumDecimal(decimalArray, ref selection), resultType),
            _ => throw new NotSupportedException($"Sum is not supported for column type {column.GetType().Name}")
        };
    }

    /// <summary>
    /// Executes an Average operation using the compact SelectionBitmap.
    /// </summary>
    public static object ExecuteAverage(IArrowArray column, ref SelectionBitmap selection, Type resultType)
    {
        return column switch
        {
            Int32Array int32Array => ConvertResult(AverageInt32(int32Array, ref selection), resultType),
            Int64Array int64Array => ConvertResult(AverageInt64(int64Array, ref selection), resultType),
            DoubleArray doubleArray => ConvertResult(AverageDouble(doubleArray, ref selection), resultType),
            FloatArray floatArray => ConvertResult(AverageFloat(floatArray, ref selection), resultType),
            Decimal128Array decimalArray => ConvertResult(AverageDecimal(decimalArray, ref selection), resultType),
            _ => throw new NotSupportedException($"Average is not supported for column type {column.GetType().Name}")
        };
    }

    /// <summary>
    /// Executes a Min operation using the compact SelectionBitmap.
    /// </summary>
    public static object ExecuteMin(IArrowArray column, ref SelectionBitmap selection, Type resultType)
    {
        return column switch
        {
            Int32Array int32Array => ConvertResult(MinInt32(int32Array, ref selection), resultType),
            Int64Array int64Array => ConvertResult(MinInt64(int64Array, ref selection), resultType),
            DoubleArray doubleArray => ConvertResult(MinDouble(doubleArray, ref selection), resultType),
            Decimal128Array decimalArray => ConvertResult(MinDecimal(decimalArray, ref selection), resultType),
            TimestampArray timestampArray => MinDateTime(timestampArray, ref selection),
            _ => throw new NotSupportedException($"Min is not supported for column type {column.GetType().Name}")
        };
    }

    /// <summary>
    /// Executes a Max operation using the compact SelectionBitmap.
    /// </summary>
    public static object ExecuteMax(IArrowArray column, ref SelectionBitmap selection, Type resultType)
    {
        return column switch
        {
            Int32Array int32Array => ConvertResult(MaxInt32(int32Array, ref selection), resultType),
            Int64Array int64Array => ConvertResult(MaxInt64(int64Array, ref selection), resultType),
            DoubleArray doubleArray => ConvertResult(MaxDouble(doubleArray, ref selection), resultType),
            Decimal128Array decimalArray => ConvertResult(MaxDecimal(decimalArray, ref selection), resultType),
            TimestampArray timestampArray => MaxDateTime(timestampArray, ref selection),
            _ => throw new NotSupportedException($"Max is not supported for column type {column.GetType().Name}")
        };
    }

    // SelectionBitmap-based aggregate implementations
    private static long SumInt32(Int32Array array, ref SelectionBitmap selection)
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
                sum += (decimal)array.GetSqlDecimal(i);
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
        return count > 0 ? (double)sum / count : 0;
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
        return count > 0 ? (double)sum / count : 0;
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
        return count > 0 ? sum / count : 0;
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
        return count > 0 ? sum / count : 0;
    }

    private static decimal AverageDecimal(Decimal128Array array, ref SelectionBitmap selection)
    {
        decimal sum = 0;
        int count = 0;
        foreach (var i in selection.GetSelectedIndices())
        {
            if (!array.IsNull(i))
            {
                sum += (decimal)array.GetSqlDecimal(i);
                count++;
            }
        }
        return count > 0 ? sum / count : 0;
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
                var value = (decimal)array.GetSqlDecimal(i);
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

    private static DateTime MinDateTime(TimestampArray array, ref SelectionBitmap selection)
    {
        DateTimeOffset min = DateTimeOffset.MaxValue;
        bool hasValue = false;
        foreach (var i in selection.GetSelectedIndices())
        {
            if (!array.IsNull(i))
            {
                var value = array.GetTimestamp(i)!.Value;
                if (!hasValue || value < min)
                {
                    min = value;
                    hasValue = true;
                }
            }
        }
        if (!hasValue) throw new InvalidOperationException("Sequence contains no elements.");
        return min.UtcDateTime;
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
                var value = (decimal)array.GetSqlDecimal(i);
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

    private static DateTime MaxDateTime(TimestampArray array, ref SelectionBitmap selection)
    {
        DateTimeOffset max = DateTimeOffset.MinValue;
        bool hasValue = false;
        foreach (var i in selection.GetSelectedIndices())
        {
            if (!array.IsNull(i))
            {
                var value = array.GetTimestamp(i)!.Value;
                if (!hasValue || value > max)
                {
                    max = value;
                    hasValue = true;
                }
            }
        }
        if (!hasValue) throw new InvalidOperationException("Sequence contains no elements.");
        return max.UtcDateTime;
    }

    #endregion
}
