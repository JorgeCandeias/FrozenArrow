using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using Apache.Arrow;

namespace FrozenArrow.Query;

/// <summary>
/// Provides SIMD-accelerated fused predicate evaluation and aggregation.
/// Processes 8 Int32 values or 4 Double values per iteration using AVX2.
/// </summary>
internal static class SimdFusedEvaluator
{
    /// <summary>
    /// SIMD-optimized fused sum for Int32 arrays.
    /// Evaluates predicates and accumulates sum in a single pass using vectorized operations.
    /// </summary>
    public static long FusedSumInt32Simd(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        Int32Array valueArray,
        int startRow,
        int endRow)
    {
        var values = valueArray.Values;
        var nullBitmap = valueArray.NullBitmapBuffer.Span;
        var hasNulls = valueArray.NullCount > 0;
        long sum = 0;
        int i = startRow;

        // Check if we can use SIMD and have compatible predicates
        if (Vector256.IsHardwareAccelerated && (endRow - startRow) >= 8 && 
            CanUseSimdPredicates(predicates, predicateColumns))
        {
            // Get Int32 predicate info for SIMD evaluation
            var int32Predicates = ExtractInt32PredicateInfo(predicates, predicateColumns);
            
            if (int32Predicates.Count > 0)
            {
                ref int valuesRef = ref Unsafe.AsRef(in values[0]);
                
                // Align to 8-element boundary
                int vectorStart = ((startRow + 7) >> 3) << 3;
                int vectorEnd = (endRow >> 3) << 3;

                // Scalar head
                for (; i < vectorStart && i < endRow; i++)
                {
                    if (hasNulls && IsNull(nullBitmap, i)) continue;
                    if (EvaluateAllPredicatesScalar(predicates, predicateColumns, i))
                    {
                        sum += values[i];
                    }
                }

                // SIMD middle - process 8 elements at a time
                for (; i < vectorEnd; i += 8)
                {
                    // Load 8 values to sum
                    var data = Vector256.LoadUnsafe(ref Unsafe.Add(ref valuesRef, i));
                    
                    // Evaluate all predicates, get combined mask
                    byte combinedMask = EvaluatePredicatesSimd8(int32Predicates, i);
                    
                    // Apply null mask if needed
                    if (hasNulls)
                    {
                        combinedMask = ApplyNullMask8(combinedMask, nullBitmap, i);
                    }
                    
                    // Accumulate matching values
                    sum += AccumulateWithMask(data, combinedMask);
                }
            }
        }

        // Scalar tail
        for (; i < endRow; i++)
        {
            if (hasNulls && IsNull(nullBitmap, i)) continue;
            if (EvaluateAllPredicatesScalar(predicates, predicateColumns, i))
            {
                sum += values[i];
            }
        }

        return sum;
    }

    /// <summary>
    /// SIMD-optimized fused sum for Double arrays.
    /// Evaluates predicates and accumulates sum using 4-wide vectorized operations.
    /// </summary>
    public static double FusedSumDoubleSimd(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        DoubleArray valueArray,
        int startRow,
        int endRow)
    {
        var values = valueArray.Values;
        var nullBitmap = valueArray.NullBitmapBuffer.Span;
        var hasNulls = valueArray.NullCount > 0;
        double sum = 0;
        int i = startRow;

        // Check if we can use SIMD and have compatible predicates
        if (Vector256.IsHardwareAccelerated && (endRow - startRow) >= 4 &&
            CanUseSimdPredicates(predicates, predicateColumns))
        {
            var doublePreds = ExtractDoublePredicateInfo(predicates, predicateColumns);
            
            if (doublePreds.Count > 0)
            {
                ref double valuesRef = ref Unsafe.AsRef(in values[0]);
                
                // Align to 4-element boundary
                int vectorStart = ((startRow + 3) >> 2) << 2;
                int vectorEnd = (endRow >> 2) << 2;

                // Scalar head
                for (; i < vectorStart && i < endRow; i++)
                {
                    if (hasNulls && IsNull(nullBitmap, i)) continue;
                    if (EvaluateAllPredicatesScalar(predicates, predicateColumns, i))
                    {
                        sum += values[i];
                    }
                }

                // SIMD middle - process 4 elements at a time
                for (; i < vectorEnd; i += 4)
                {
                    var data = Vector256.LoadUnsafe(ref Unsafe.Add(ref valuesRef, i));
                    
                    byte combinedMask = EvaluatePredicatesSimd4Double(doublePreds, i);
                    
                    if (hasNulls)
                    {
                        combinedMask = ApplyNullMask4(combinedMask, nullBitmap, i);
                    }
                    
                    sum += AccumulateDoubleWithMask(data, combinedMask);
                }
            }
        }

        // Scalar tail
        for (; i < endRow; i++)
        {
            if (hasNulls && IsNull(nullBitmap, i)) continue;
            if (EvaluateAllPredicatesScalar(predicates, predicateColumns, i))
            {
                sum += values[i];
            }
        }

        return sum;
    }

    /// <summary>
    /// SIMD-optimized fused count.
    /// </summary>
    public static int FusedCountSimd(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        int startRow,
        int endRow)
    {
        int count = 0;
        int i = startRow;

        if (Vector256.IsHardwareAccelerated && (endRow - startRow) >= 8 &&
            CanUseSimdPredicates(predicates, predicateColumns))
        {
            var int32Predicates = ExtractInt32PredicateInfo(predicates, predicateColumns);
            
            if (int32Predicates.Count > 0)
            {
                // Align to 8-element boundary
                int vectorStart = ((startRow + 7) >> 3) << 3;
                int vectorEnd = (endRow >> 3) << 3;

                // Scalar head
                for (; i < vectorStart && i < endRow; i++)
                {
                    if (EvaluateAllPredicatesScalar(predicates, predicateColumns, i))
                    {
                        count++;
                    }
                }

                // SIMD middle
                for (; i < vectorEnd; i += 8)
                {
                    byte combinedMask = EvaluatePredicatesSimd8(int32Predicates, i);
                    count += BitOperations.PopCount(combinedMask);
                }
            }
        }

        // Scalar tail
        for (; i < endRow; i++)
        {
            if (EvaluateAllPredicatesScalar(predicates, predicateColumns, i))
            {
                count++;
            }
        }

        return count;
    }

    #region Predicate Info Extraction

    /// <summary>
    /// Checks if all predicates can be evaluated using SIMD.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool CanUseSimdPredicates(IReadOnlyList<ColumnPredicate> predicates, IArrowArray[] columns)
    {
        // For now, require at least one Int32 or Double comparison predicate
        for (int p = 0; p < predicates.Count; p++)
        {
            if (predicates[p] is Int32ComparisonPredicate && columns[p] is Int32Array)
                return true;
            if (predicates[p] is DoubleComparisonPredicate && columns[p] is DoubleArray)
                return true;
        }
        return false;
    }

    private static List<(Int32Array Array, Vector256<int> CompareValue, ComparisonOperator Op)> ExtractInt32PredicateInfo(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] columns)
    {
        var result = new List<(Int32Array, Vector256<int>, ComparisonOperator)>();
        
        
        for (int p = 0; p < predicates.Count; p++)
        {
            if (predicates[p] is Int32ComparisonPredicate int32Pred && columns[p] is Int32Array int32Array)
            {
                result.Add((int32Array, Vector256.Create(int32Pred.Value), int32Pred.Operator));
            }
        }
        
        return result;
    }

    private static List<(DoubleArray Array, Vector256<double> CompareValue, ComparisonOperator Op)> ExtractDoublePredicateInfo(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] columns)
    {
        var result = new List<(DoubleArray, Vector256<double>, ComparisonOperator)>();
        
        for (int p = 0; p < predicates.Count; p++)
        {
            if (predicates[p] is DoubleComparisonPredicate doublePred && columns[p] is DoubleArray doubleArray)
            {
                result.Add((doubleArray, Vector256.Create(doublePred.Value), doublePred.Operator));
            }
        }
        
        return result;
    }

    #endregion

    #region SIMD Predicate Evaluation

    /// <summary>
    /// Evaluates all Int32 predicates for 8 consecutive rows using SIMD.
    /// Returns an 8-bit mask where 1 = all predicates passed for that row.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static byte EvaluatePredicatesSimd8(
        List<(Int32Array Array, Vector256<int> CompareValue, ComparisonOperator Op)> predicates,
        int startIndex)
    {
        byte combinedMask = 0xFF; // Start with all rows passing
        
        foreach (var (array, compareValue, op) in predicates)
        {
            var values = array.Values;
            ref int valuesRef = ref Unsafe.AsRef(in values[0]);
            var data = Vector256.LoadUnsafe(ref Unsafe.Add(ref valuesRef, startIndex));
            
            // Perform comparison
            Vector256<int> resultMask = op switch
            {
                ComparisonOperator.Equal => Vector256.Equals(data, compareValue),
                ComparisonOperator.NotEqual => ~Vector256.Equals(data, compareValue),
                ComparisonOperator.LessThan => Vector256.LessThan(data, compareValue),
                ComparisonOperator.LessThanOrEqual => Vector256.LessThanOrEqual(data, compareValue),
                ComparisonOperator.GreaterThan => Vector256.GreaterThan(data, compareValue),
                ComparisonOperator.GreaterThanOrEqual => Vector256.GreaterThanOrEqual(data, compareValue),
                _ => Vector256<int>.AllBitsSet
            };
            
            // Convert to byte mask using MoveMask
            byte predicateMask = (byte)Avx.MoveMask(resultMask.AsSingle());
            
            // Handle nulls for this predicate column
            if (array.NullCount > 0)
            {
                predicateMask = ApplyNullMask8(predicateMask, array.NullBitmapBuffer.Span, startIndex);
            }
            
            // AND with combined mask
            combinedMask &= predicateMask;
            
            // Early exit if no rows pass
            if (combinedMask == 0) break;
        }
        
        return combinedMask;
    }

    /// <summary>
    /// Evaluates all Double predicates for 4 consecutive rows using SIMD.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static byte EvaluatePredicatesSimd4Double(
        List<(DoubleArray Array, Vector256<double> CompareValue, ComparisonOperator Op)> predicates,
        int startIndex)
    {
        byte combinedMask = 0x0F; // 4 bits for 4 rows
        
        foreach (var (array, compareValue, op) in predicates)
        {
            var values = array.Values;
            ref double valuesRef = ref Unsafe.AsRef(in values[0]);
            var data = Vector256.LoadUnsafe(ref Unsafe.Add(ref valuesRef, startIndex));
            
            Vector256<double> resultMask = op switch
            {
                ComparisonOperator.Equal => Vector256.Equals(data, compareValue),
                ComparisonOperator.NotEqual => ~Vector256.Equals(data, compareValue),
                ComparisonOperator.LessThan => Vector256.LessThan(data, compareValue),
                ComparisonOperator.LessThanOrEqual => Vector256.LessThanOrEqual(data, compareValue),
                ComparisonOperator.GreaterThan => Vector256.GreaterThan(data, compareValue),
                ComparisonOperator.GreaterThanOrEqual => Vector256.GreaterThanOrEqual(data, compareValue),
                _ => Vector256<double>.AllBitsSet
            };
            
            byte predicateMask = (byte)Avx.MoveMask(resultMask);
            
            if (array.NullCount > 0)
            {
                predicateMask = ApplyNullMask4(predicateMask, array.NullBitmapBuffer.Span, startIndex);
            }
            
            combinedMask &= predicateMask;
            if (combinedMask == 0) break;
        }
        
        return combinedMask;
    }

    #endregion

    #region Masked Accumulation

    /// <summary>
    /// Accumulates Int32 values where the corresponding mask bit is set.
    /// Uses branch-free extraction for performance.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long AccumulateWithMask(Vector256<int> data, byte mask)
    {
        if (mask == 0) return 0;
        if (mask == 0xFF)
        {
            // All 8 values match - use horizontal sum
            // Sum pairs: 4x int pairs -> 4x long
            var lowerHalf = data.GetLower().AsInt64();
            var upperHalf = data.GetUpper().AsInt64();
            
            // Extract and sum all 8 values
            long sum = 0;
            for (int j = 0; j < 8; j++)
            {
                sum += data.GetElement(j);
            }
            return sum;
        }
        
        // Sparse case: iterate through set bits
        long result = 0;
        while (mask != 0)
        {
            int idx = BitOperations.TrailingZeroCount(mask);
            result += data.GetElement(idx);
            mask &= (byte)(mask - 1); // Clear lowest bit
        }
        return result;
    }

    /// <summary>
    /// Accumulates Double values where the corresponding mask bit is set.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static double AccumulateDoubleWithMask(Vector256<double> data, byte mask)
    {
        if (mask == 0) return 0;
        if (mask == 0x0F)
        {
            // All 4 values match - horizontal sum
            double sum = 0;
            for (int j = 0; j < 4; j++)
            {
                sum += data.GetElement(j);
            }
            return sum;
        }
        
        // Sparse case
        double result = 0;
        while (mask != 0)
        {
            int idx = BitOperations.TrailingZeroCount(mask);
            result += data.GetElement(idx);
            mask &= (byte)(mask - 1);
        }
        return result;
    }

    #endregion

    #region Null Bitmap Helpers

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static byte ApplyNullMask8(byte mask, ReadOnlySpan<byte> nullBitmap, int startIndex)
    {
        if (nullBitmap.IsEmpty) return mask;
        
        var byteIndex = startIndex >> 3;
        var bitOffset = startIndex & 7;
        
        byte nullMask;
        if (bitOffset == 0)
        {
            nullMask = byteIndex < nullBitmap.Length ? nullBitmap[byteIndex] : (byte)0xFF;
        }
        else
        {
            var lowByte = byteIndex < nullBitmap.Length ? nullBitmap[byteIndex] : (byte)0xFF;
            var highByte = (byteIndex + 1) < nullBitmap.Length ? nullBitmap[byteIndex + 1] : (byte)0xFF;
            nullMask = (byte)((lowByte >> bitOffset) | (highByte << (8 - bitOffset)));
        }
        
        return (byte)(mask & nullMask);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static byte ApplyNullMask4(byte mask, ReadOnlySpan<byte> nullBitmap, int startIndex)
    {
        if (nullBitmap.IsEmpty) return mask;
        
        var byteIndex = startIndex >> 3;
        var bitOffset = startIndex & 7;
        
        byte nullMask;
        if (bitOffset <= 4)
        {
            nullMask = byteIndex < nullBitmap.Length ? (byte)(nullBitmap[byteIndex] >> bitOffset) : (byte)0x0F;
        }
        else
        {
            var lowByte = byteIndex < nullBitmap.Length ? nullBitmap[byteIndex] : (byte)0xFF;
            var highByte = (byteIndex + 1) < nullBitmap.Length ? nullBitmap[byteIndex + 1] : (byte)0xFF;
            nullMask = (byte)((lowByte >> bitOffset) | (highByte << (8 - bitOffset)));
        }
        
        return (byte)(mask & (nullMask & 0x0F));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsNull(ReadOnlySpan<byte> nullBitmap, int index)
    {
        if (nullBitmap.IsEmpty) return false;
        return (nullBitmap[index >> 3] & (1 << (index & 7))) == 0;
    }

    #endregion

    #region Scalar Fallback

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool EvaluateAllPredicatesScalar(
        IReadOnlyList<ColumnPredicate> predicates,
        IArrowArray[] predicateColumns,
        int rowIndex)
    {
        for (int p = 0; p < predicates.Count; p++)
        {
            if (!predicates[p].EvaluateSingleRow(predicateColumns[p], rowIndex))
            {
                return false;
            }
        }
        return true;
    }

    #endregion
}
