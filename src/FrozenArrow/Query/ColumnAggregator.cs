using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using Apache.Arrow;

namespace FrozenArrow.Query;

/// <summary>
/// Performs aggregate operations directly on Arrow columns without materializing rows.
/// Uses SIMD vectorization for dense selections when beneficial.
/// </summary>
internal static class ColumnAggregator
{
    // Threshold for using SIMD dense path vs sparse iteration
    // If more than 50% of rows are selected, use dense SIMD path
    private const double DenseThreshold = 0.5;

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
                sum += array.GetValue(i)!.Value;
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
        return count > 0 ? (double)sum / count : throw new InvalidOperationException("Sequence contains no elements");
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
        return count > 0 ? (double)sum / count : throw new InvalidOperationException("Sequence contains no elements");
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
        return count > 0 ? sum / count : throw new InvalidOperationException("Sequence contains no elements");
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
        return count > 0 ? sum / count : throw new InvalidOperationException("Sequence contains no elements");
    }

    public static decimal AverageDecimal(Decimal128Array array, bool[] selection)
    {
        decimal sum = 0;
        int count = 0;
        for (int i = 0; i < array.Length; i++)
        {
            if (selection[i] && !array.IsNull(i))
            {
                sum += array.GetValue(i)!.Value;
                count++;
            }
        }
        return count > 0 ? sum / count : throw new InvalidOperationException("Sequence contains no elements");
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
    /// Uses SIMD block-based aggregation for optimal performance.
    /// </summary>
    public static object ExecuteSum(IArrowArray column, ref SelectionBitmap selection, Type resultType)
    {
        // Use the new SIMD block-based aggregator for supported types
        return column switch
        {
            Int32Array int32Array => SimdBlockAggregator.ExecuteSum(int32Array, ref selection, resultType),
            Int64Array int64Array => SimdBlockAggregator.ExecuteSum(int64Array, ref selection, resultType),
            DoubleArray doubleArray => SimdBlockAggregator.ExecuteSum(doubleArray, ref selection, resultType),
            FloatArray floatArray => ConvertResult(SumFloat(floatArray, ref selection), resultType),
            Decimal128Array decimalArray => SimdBlockAggregator.ExecuteSum(decimalArray, ref selection, resultType),
            DictionaryArray dictArray => ConvertResult(SumDictionary(dictArray, ref selection), resultType),
            _ => throw new NotSupportedException($"Sum is not supported for column type {column.GetType().Name}")
        };
    }

    /// <summary>
    /// Executes an Average operation using the compact SelectionBitmap.
    /// Uses SIMD block-based aggregation for optimal performance.
    /// </summary>
    public static object ExecuteAverage(IArrowArray column, ref SelectionBitmap selection, Type resultType)
    {
        // Use the new SIMD block-based aggregator for supported types
        return column switch
        {
            Int32Array int32Array => SimdBlockAggregator.ExecuteAverage(int32Array, ref selection, resultType),
            Int64Array int64Array => SimdBlockAggregator.ExecuteAverage(int64Array, ref selection, resultType),
            DoubleArray doubleArray => SimdBlockAggregator.ExecuteAverage(doubleArray, ref selection, resultType),
            FloatArray floatArray => ConvertResult(AverageFloat(floatArray, ref selection), resultType),
            Decimal128Array decimalArray => SimdBlockAggregator.ExecuteAverage(decimalArray, ref selection, resultType),
            DictionaryArray dictArray => ConvertResult(AverageDictionary(dictArray, ref selection), resultType),
            _ => throw new NotSupportedException($"Average is not supported for column type {column.GetType().Name}")
        };
    }

    /// <summary>
    /// Executes a Min operation using the compact SelectionBitmap.
    /// Uses SIMD block-based aggregation for optimal performance.
    /// </summary>
    public static object ExecuteMin(IArrowArray column, ref SelectionBitmap selection, Type resultType)
    {
        // Use the new SIMD block-based aggregator for supported types
        return column switch
        {
            Int32Array int32Array => SimdBlockAggregator.ExecuteMin(int32Array, ref selection, resultType),
            Int64Array int64Array => SimdBlockAggregator.ExecuteMin(int64Array, ref selection, resultType),
            DoubleArray doubleArray => SimdBlockAggregator.ExecuteMin(doubleArray, ref selection, resultType),
            Decimal128Array decimalArray => SimdBlockAggregator.ExecuteMin(decimalArray, ref selection, resultType),
            TimestampArray timestampArray => MinDateTime(timestampArray, ref selection),
            DictionaryArray dictArray => ConvertResult(MinDictionary(dictArray, ref selection), resultType),
            _ => throw new NotSupportedException($"Min is not supported for column type {column.GetType().Name}")
        };
    }

    /// <summary>
    /// Executes a Max operation using the compact SelectionBitmap.
    /// Uses SIMD block-based aggregation for optimal performance.
    /// </summary>
    public static object ExecuteMax(IArrowArray column, ref SelectionBitmap selection, Type resultType)
    {
        // Use the new SIMD block-based aggregator for supported types
        return column switch
        {
            Int32Array int32Array => SimdBlockAggregator.ExecuteMax(int32Array, ref selection, resultType),
            Int64Array int64Array => SimdBlockAggregator.ExecuteMax(int64Array, ref selection, resultType),
            DoubleArray doubleArray => SimdBlockAggregator.ExecuteMax(doubleArray, ref selection, resultType),
            Decimal128Array decimalArray => SimdBlockAggregator.ExecuteMax(decimalArray, ref selection, resultType),
            TimestampArray timestampArray => MaxDateTime(timestampArray, ref selection),
            DictionaryArray dictArray => ConvertResult(MaxDictionary(dictArray, ref selection), resultType),
            _ => throw new NotSupportedException($"Max is not supported for column type {column.GetType().Name}")
        };
    }

    // SelectionBitmap-based aggregate implementations
    // Choose between dense SIMD path or sparse iteration based on selectivity
    
    private static long SumInt32(Int32Array array, ref SelectionBitmap selection)
    {
        var length = array.Length;
        var selectedCount = selection.CountSet();
        
        // For dense selections, use SIMD
        if (selectedCount > length * DenseThreshold && array.NullCount == 0)
        {
            return SumInt32Simd(array, ref selection);
        }
        
        // Sparse path: iterate through selected indices
        long sum = 0;
        var span = array.Values;
        foreach (var i in selection.GetSelectedIndices())
        {
            if (!array.IsNull(i))
                sum += span[i];
        }
        return sum;
    }

    private static long SumInt32Simd(Int32Array array, ref SelectionBitmap selection)
    {
        var span = array.Values;
        var length = array.Length;
        long sum = 0;
        int i = 0;

        // AVX2 path: process 8 int32s at a time
        if (Vector256.IsHardwareAccelerated && length >= 8)
        {
            ref int valuesRef = ref Unsafe.AsRef(in span[0]);
            var sumVecLo = Vector256<long>.Zero;
            var sumVecHi = Vector256<long>.Zero;
            int vectorEnd = length - (length % 8);

            for (; i < vectorEnd; i += 8)
            {
                // Check if all 8 values are selected by checking the bitmap block
                // This is a simplification - for full correctness we'd need to check each bit
                var data = Vector256.LoadUnsafe(ref Unsafe.Add(ref valuesRef, i));
                
                // Create a mask from the selection bitmap
                var maskLo = CreateSelectionMask4(ref selection, i);
                var maskHi = CreateSelectionMask4(ref selection, i + 4);
                
                // Widen to long for accumulation to avoid overflow
                var (lo, hi) = Vector256.Widen(data);
                
                // Apply selection mask and accumulate
                sumVecLo = Vector256.Add(sumVecLo, Vector256.BitwiseAnd(lo, maskLo));
                sumVecHi = Vector256.Add(sumVecHi, Vector256.BitwiseAnd(hi, maskHi));
            }
            
            sum = Vector256.Sum(sumVecLo) + Vector256.Sum(sumVecHi);
        }

        // Scalar tail
        for (; i < length; i++)
        {
            if (selection[i])
                sum += span[i];
        }
        
        return sum;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Vector256<long> CreateSelectionMask4(ref SelectionBitmap selection, int startIndex)
    {
        // Create a 4-element long mask from 4 bitmap bits
        // Returns 0xFFFFFFFFFFFFFFFF for selected, 0 for not selected
        return Vector256.Create(
            selection[startIndex] ? -1L : 0L,
            selection[startIndex + 1] ? -1L : 0L,
            selection[startIndex + 2] ? -1L : 0L,
            selection[startIndex + 3] ? -1L : 0L
        );
    }

    private static long SumInt64(Int64Array array, ref SelectionBitmap selection)
    {
        var length = array.Length;
        var selectedCount = selection.CountSet();
        
        // For dense selections, use SIMD
        if (selectedCount > length * DenseThreshold && array.NullCount == 0)
        {
            return SumInt64Simd(array, ref selection);
        }
        
        long sum = 0;
        var span = array.Values;
        foreach (var i in selection.GetSelectedIndices())
        {
            if (!array.IsNull(i))
                sum += span[i];
        }
        return sum;
    }

    private static long SumInt64Simd(Int64Array array, ref SelectionBitmap selection)
    {
        var span = array.Values;
        var length = array.Length;
        long sum = 0;
        int i = 0;

        if (Vector256.IsHardwareAccelerated && length >= 4)
        {
            ref long valuesRef = ref Unsafe.AsRef(in span[0]);
            var sumVec = Vector256<long>.Zero;
            int vectorEnd = length - (length % 4);

            for (; i < vectorEnd; i += 4)
            {
                var data = Vector256.LoadUnsafe(ref Unsafe.Add(ref valuesRef, i));
                var mask = CreateSelectionMask4(ref selection, i);
                sumVec = Vector256.Add(sumVec, Vector256.BitwiseAnd(data, mask));
            }
            
            sum = Vector256.Sum(sumVec);
        }

        for (; i < length; i++)
        {
            if (selection[i])
                sum += span[i];
        }
        
        return sum;
    }

    private static double SumDouble(DoubleArray array, ref SelectionBitmap selection)
    {
        var length = array.Length;
        var selectedCount = selection.CountSet();
        
        // For dense selections, use SIMD
        if (selectedCount > length * DenseThreshold && array.NullCount == 0)
        {
            return SumDoubleSimd(array, ref selection);
        }
        
        double sum = 0;
        var span = array.Values;
        foreach (var i in selection.GetSelectedIndices())
        {
            if (!array.IsNull(i))
                sum += span[i];
        }
        return sum;
    }

    private static double SumDoubleSimd(DoubleArray array, ref SelectionBitmap selection)
    {
        var span = array.Values;
        var length = array.Length;
        double sum = 0;
        int i = 0;

        if (Vector256.IsHardwareAccelerated && length >= 4)
        {
            ref double valuesRef = ref Unsafe.AsRef(in span[0]);
            var sumVec = Vector256<double>.Zero;
            int vectorEnd = length - (length % 4);

            for (; i < vectorEnd; i += 4)
            {
                var data = Vector256.LoadUnsafe(ref Unsafe.Add(ref valuesRef, i));
                var mask = CreateDoubleMask4(ref selection, i);
                sumVec = Vector256.Add(sumVec, Vector256.BitwiseAnd(data, mask));
            }
            
            sum = Vector256.Sum(sumVec);
        }

        for (; i < length; i++)
        {
            if (selection[i])
                sum += span[i];
        }
        
        return sum;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Vector256<double> CreateDoubleMask4(ref SelectionBitmap selection, int startIndex)
    {
        return Vector256.Create(
            BitConverter.Int64BitsToDouble(selection[startIndex] ? -1L : 0L),
            BitConverter.Int64BitsToDouble(selection[startIndex + 1] ? -1L : 0L),
            BitConverter.Int64BitsToDouble(selection[startIndex + 2] ? -1L : 0L),
            BitConverter.Int64BitsToDouble(selection[startIndex + 3] ? -1L : 0L)
        );
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

