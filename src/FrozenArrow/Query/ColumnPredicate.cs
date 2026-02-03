using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using Apache.Arrow;

namespace FrozenArrow.Query;

/// <summary>
/// Base class for predicates that can be evaluated directly against Arrow columns
/// without materializing objects.
/// </summary>
public abstract class ColumnPredicate
{
    /// <summary>
    /// Gets the name of the column this predicate operates on.
    /// </summary>
    public abstract string ColumnName { get; }

    /// <summary>
    /// Gets the column index in the record batch.
    /// </summary>
    public int ColumnIndex { get; internal set; } = -1;

    /// <summary>
    /// Evaluates this predicate against the column, updating the selection bitmap.
    /// Only rows that are already selected (true) will be evaluated.
    /// </summary>
    /// <param name="batch">The Arrow record batch.</param>
    /// <param name="selection">The selection bitmap to update. True = selected, False = filtered out.</param>
    public abstract void Evaluate(RecordBatch batch, Span<bool> selection);

    /// <summary>
    /// Evaluates this predicate against the column, updating the selection bitmap.
    /// This overload is used when the selection is stored in an array.
    /// </summary>
    public void Evaluate(RecordBatch batch, bool[] selection)
    {
        Evaluate(batch, selection.AsSpan());
    }

    /// <summary>
    /// Evaluates this predicate against the column, updating the compact selection bitmap.
    /// </summary>
    /// <param name="batch">The Arrow record batch.</param>
    /// <param name="selection">The compact selection bitmap to update.</param>
    public virtual void Evaluate(RecordBatch batch, ref SelectionBitmap selection)
    {
        var column = batch.Column(ColumnIndex);
        EvaluateRange(column, ref selection, 0, batch.Length);
    }

    /// <summary>
    /// Evaluates this predicate for a range of rows. This method is used for parallel execution.
    /// </summary>
    /// <param name="column">The column to evaluate against.</param>
    /// <param name="selection">The selection bitmap to update.</param>
    /// <param name="startIndex">The first row index (inclusive).</param>
    /// <param name="endIndex">The last row index (exclusive).</param>
    /// <remarks>
    /// This method is thread-safe for non-overlapping ranges, allowing multiple threads
    /// to evaluate different row ranges concurrently on the same bitmap.
    /// </remarks>
    public virtual void EvaluateRange(IArrowArray column, ref SelectionBitmap selection, int startIndex, int endIndex)
    {
        // Default implementation: iterate and update bitmap
        // Subclasses can override for better performance (e.g., SIMD)
        for (int i = startIndex; i < endIndex; i++)
        {
            if (!selection[i]) continue; // Already filtered out
            
            // Evaluate using single-item logic
            if (!EvaluateSingle(column, i))
            {
                selection.Clear(i);
            }
        }
    }

    /// <summary>
    /// Evaluates this predicate for a single row.
    /// Override in subclasses for optimized bitmap evaluation.
    /// </summary>
    protected virtual bool EvaluateSingle(IArrowArray column, int index)
    {
        // Default: not implemented, subclasses should override EvaluateRange instead
        throw new NotImplementedException("Subclass must override either EvaluateSingle or EvaluateRange");
    }
}

/// <summary>
/// Comparison operators for numeric predicates.
/// </summary>
public enum ComparisonOperator
{
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual
}

/// <summary>
/// Predicate for comparing an Int32 column against a constant value.
/// Uses SIMD vectorization for bulk comparison when processing primitive Int32Arrays.
/// </summary>
public sealed class Int32ComparisonPredicate : ColumnPredicate
{
    public override string ColumnName { get; }
    public ComparisonOperator Operator { get; }
    public int Value { get; }

    public Int32ComparisonPredicate(string columnName, ComparisonOperator op, int value)
    {
        ColumnName = columnName;
        Operator = op;
        Value = value;
    }

    public override void Evaluate(RecordBatch batch, Span<bool> selection)
    {
        var column = batch.Column(ColumnIndex);
        var length = batch.Length;

        for (int i = 0; i < length; i++)
        {
            if (!selection[i]) continue; // Already filtered out

            if (column.IsNull(i))
            {
                selection[i] = false;
                continue;
            }

            var columnValue = RunLengthEncodedArrayBuilder.GetInt32Value(column, i);
            selection[i] = Operator switch
            {
                ComparisonOperator.Equal => columnValue == Value,
                ComparisonOperator.NotEqual => columnValue != Value,
                ComparisonOperator.LessThan => columnValue < Value,
                ComparisonOperator.LessThanOrEqual => columnValue <= Value,
                ComparisonOperator.GreaterThan => columnValue > Value,
                ComparisonOperator.GreaterThanOrEqual => columnValue >= Value,
                _ => false
            };
        }
    }

    /// <summary>
    /// SIMD-optimized evaluation for SelectionBitmap.
    /// Processes 8 Int32 values per iteration using AVX2 when available.
    /// </summary>
    public override void Evaluate(RecordBatch batch, ref SelectionBitmap selection)
    {
        var column = batch.Column(ColumnIndex);
        
        // For primitive Int32Array, use optimized SIMD path
        if (column is Int32Array int32Array)
        {
            EvaluateInt32ArraySimd(int32Array, ref selection);
            return;
        }

        // Fallback to scalar path for dictionary-encoded or other array types
        base.Evaluate(batch, ref selection);
    }

    private void EvaluateInt32ArraySimd(Int32Array array, ref SelectionBitmap selection)
    {
        var values = array.Values;
        var length = array.Length;
        var nullBitmap = array.NullBitmapBuffer.Span;
        var hasNulls = array.NullCount > 0;

        // SIMD path: process 8 elements at a time with AVX2
        if (Vector256.IsHardwareAccelerated && length >= 8)
        {
            var compareValue = Vector256.Create(Value);
            ref int valuesRef = ref Unsafe.AsRef(in values[0]);
            int i = 0;
            int vectorEnd = length - (length % 8);

            for (; i < vectorEnd; i += 8)
            {
                // Load 8 values
                var data = Vector256.LoadUnsafe(ref Unsafe.Add(ref valuesRef, i));
                
                // Perform comparison based on operator
                Vector256<int> mask = Operator switch
                {
                    ComparisonOperator.Equal => Vector256.Equals(data, compareValue),
                    ComparisonOperator.NotEqual => ~Vector256.Equals(data, compareValue),
                    ComparisonOperator.LessThan => Vector256.LessThan(data, compareValue),
                    ComparisonOperator.LessThanOrEqual => Vector256.LessThanOrEqual(data, compareValue),
                    ComparisonOperator.GreaterThan => Vector256.GreaterThan(data, compareValue),
                    ComparisonOperator.GreaterThanOrEqual => Vector256.GreaterThanOrEqual(data, compareValue),
                    _ => Vector256<int>.Zero
                };

                // Extract comparison results and apply to bitmap
                // Each comparison produces 0xFFFFFFFF for true, 0x00000000 for false
                // We need to convert this to individual bits for the bitmap
                ApplyMaskToBitmap(mask, ref selection, i, hasNulls, nullBitmap);
            }

            // Scalar tail
            for (; i < length; i++)
            {
                if (!selection[i]) continue;
                if (hasNulls && IsNull(nullBitmap, i))
                {
                    selection.Clear(i);
                    continue;
                }
                if (!EvaluateScalar(values[i]))
                {
                    selection.Clear(i);
                }
            }
        }
        else
        {
            // Scalar fallback for non-AVX2 systems
            for (int i = 0; i < length; i++)
            {
                if (!selection[i]) continue;
                if (hasNulls && IsNull(nullBitmap, i))
                {
                    selection.Clear(i);
                    continue;
                }
                if (!EvaluateScalar(values[i]))
                {
                    selection.Clear(i);
                }
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ApplyMaskToBitmap(Vector256<int> mask, ref SelectionBitmap selection, int startIndex, bool hasNulls, ReadOnlySpan<byte> nullBitmap)
    {
        // Extract each element and apply to bitmap
        // This is the "scatter" operation - unfortunately no single instruction for this
        for (int j = 0; j < 8; j++)
        {
            var idx = startIndex + j;
            if (!selection[idx]) continue;
            
            if (hasNulls && IsNull(nullBitmap, idx))
            {
                selection.Clear(idx);
                continue;
            }
            
            // mask[j] is 0xFFFFFFFF if true, 0x00000000 if false
            if (mask.GetElement(j) == 0)
            {
                selection.Clear(idx);
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsNull(ReadOnlySpan<byte> nullBitmap, int index)
    {
        if (nullBitmap.IsEmpty) return false;
        return (nullBitmap[index >> 3] & (1 << (index & 7))) == 0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool EvaluateScalar(int columnValue)
    {
        return Operator switch
        {
            ComparisonOperator.Equal => columnValue == Value,
            ComparisonOperator.NotEqual => columnValue != Value,
            ComparisonOperator.LessThan => columnValue < Value,
            ComparisonOperator.LessThanOrEqual => columnValue <= Value,
            ComparisonOperator.GreaterThan => columnValue > Value,
            ComparisonOperator.GreaterThanOrEqual => columnValue >= Value,
            _ => false
        };
    }

    protected override bool EvaluateSingle(IArrowArray column, int index)
    {
        if (column.IsNull(index)) return false;
        var columnValue = RunLengthEncodedArrayBuilder.GetInt32Value(column, index);
        return EvaluateScalar(columnValue);
    }

    /// <summary>
    /// Evaluates this predicate for a range of rows with SIMD optimization.
    /// Thread-safe for non-overlapping ranges.
    /// </summary>
    public override void EvaluateRange(IArrowArray column, ref SelectionBitmap selection, int startIndex, int endIndex)
    {
        if (column is Int32Array int32Array)
        {
            EvaluateInt32RangeSimd(int32Array, ref selection, startIndex, endIndex);
            return;
        }

        // Fallback to scalar path
        base.EvaluateRange(column, ref selection, startIndex, endIndex);
    }

    private void EvaluateInt32RangeSimd(Int32Array array, ref SelectionBitmap selection, int startIndex, int endIndex)
    {
        var values = array.Values;
        var nullBitmap = array.NullBitmapBuffer.Span;
        var hasNulls = array.NullCount > 0;
        int i = startIndex;

        // SIMD path: process 8 elements at a time with AVX2
        if (Vector256.IsHardwareAccelerated && (endIndex - startIndex) >= 8)
        {
            var compareValue = Vector256.Create(Value);
            ref int valuesRef = ref Unsafe.AsRef(in values[0]);
            
            // Align to 8-element boundary from startIndex
            int vectorStart = ((startIndex + 7) >> 3) << 3; // Round up to next 8
            int vectorEnd = (endIndex >> 3) << 3; // Round down to 8

            // Scalar head (before aligned start)
            for (; i < vectorStart && i < endIndex; i++)
            {
                if (!selection[i]) continue;
                if (hasNulls && IsNull(nullBitmap, i))
                {
                    selection.Clear(i);
                    continue;
                }
                if (!EvaluateScalar(values[i]))
                {
                    selection.Clear(i);
                }
            }

            // SIMD middle
            for (; i < vectorEnd; i += 8)
            {
                var data = Vector256.LoadUnsafe(ref Unsafe.Add(ref valuesRef, i));
                
                Vector256<int> mask = Operator switch
                {
                    ComparisonOperator.Equal => Vector256.Equals(data, compareValue),
                    ComparisonOperator.NotEqual => ~Vector256.Equals(data, compareValue),
                    ComparisonOperator.LessThan => Vector256.LessThan(data, compareValue),
                    ComparisonOperator.LessThanOrEqual => Vector256.LessThanOrEqual(data, compareValue),
                    ComparisonOperator.GreaterThan => Vector256.GreaterThan(data, compareValue),
                    ComparisonOperator.GreaterThanOrEqual => Vector256.GreaterThanOrEqual(data, compareValue),
                    _ => Vector256<int>.Zero
                };

                ApplyMaskToBitmap(mask, ref selection, i, hasNulls, nullBitmap);
            }
        }

        // Scalar tail
        for (; i < endIndex; i++)
        {
            if (!selection[i]) continue;
            if (hasNulls && IsNull(nullBitmap, i))
            {
                selection.Clear(i);
                continue;
            }
            if (!EvaluateScalar(values[i]))
            {
                selection.Clear(i);
            }
        }
    }
}

/// <summary>
/// Predicate for comparing a Double column against a constant value.
/// Uses SIMD vectorization for bulk comparison when processing primitive DoubleArrays.
/// </summary>
public sealed class DoubleComparisonPredicate : ColumnPredicate
{
    public override string ColumnName { get; }
    public ComparisonOperator Operator { get; }
    public double Value { get; }

    public DoubleComparisonPredicate(string columnName, ComparisonOperator op, double value)
    {
        ColumnName = columnName;
        Operator = op;
        Value = value;
    }

    public override void Evaluate(RecordBatch batch, Span<bool> selection)
    {
        var column = batch.Column(ColumnIndex);
        var length = batch.Length;

        for (int i = 0; i < length; i++)
        {
            if (!selection[i]) continue;

            if (column.IsNull(i))
            {
                selection[i] = false;
                continue;
            }

            var columnValue = RunLengthEncodedArrayBuilder.GetDoubleValue(column, i);
            selection[i] = Operator switch
            {
                ComparisonOperator.Equal => columnValue == Value,
                ComparisonOperator.NotEqual => columnValue != Value,
                ComparisonOperator.LessThan => columnValue < Value,
                ComparisonOperator.LessThanOrEqual => columnValue <= Value,
                ComparisonOperator.GreaterThan => columnValue > Value,
                ComparisonOperator.GreaterThanOrEqual => columnValue >= Value,
                _ => false
            };
        }
    }

    /// <summary>
    /// SIMD-optimized evaluation for SelectionBitmap.
    /// Processes 4 double values per iteration using AVX2 when available.
    /// </summary>
    public override void Evaluate(RecordBatch batch, ref SelectionBitmap selection)
    {
        var column = batch.Column(ColumnIndex);
        
        // For primitive DoubleArray, use optimized SIMD path
        if (column is DoubleArray doubleArray)
        {
            EvaluateDoubleArraySimd(doubleArray, ref selection);
            return;
        }

        // Fallback to scalar path for dictionary-encoded or other array types
        base.Evaluate(batch, ref selection);
    }

    private void EvaluateDoubleArraySimd(DoubleArray array, ref SelectionBitmap selection)
    {
        var values = array.Values;
        var length = array.Length;
        var nullBitmap = array.NullBitmapBuffer.Span;
        var hasNulls = array.NullCount > 0;

        // SIMD path: process 4 elements at a time with AVX2 (256-bit = 4 doubles)
        if (Vector256.IsHardwareAccelerated && length >= 4)
        {
            var compareValue = Vector256.Create(Value);
            ref double valuesRef = ref Unsafe.AsRef(in values[0]);
            int i = 0;
            int vectorEnd = length - (length % 4);

            for (; i < vectorEnd; i += 4)
            {
                // Load 4 values
                var data = Vector256.LoadUnsafe(ref Unsafe.Add(ref valuesRef, i));
                
                // Perform comparison based on operator
                Vector256<double> mask = Operator switch
                {
                    ComparisonOperator.Equal => Vector256.Equals(data, compareValue),
                    ComparisonOperator.NotEqual => ~Vector256.Equals(data, compareValue),
                    ComparisonOperator.LessThan => Vector256.LessThan(data, compareValue),
                    ComparisonOperator.LessThanOrEqual => Vector256.LessThanOrEqual(data, compareValue),
                    ComparisonOperator.GreaterThan => Vector256.GreaterThan(data, compareValue),
                    ComparisonOperator.GreaterThanOrEqual => Vector256.GreaterThanOrEqual(data, compareValue),
                    _ => Vector256<double>.Zero
                };

                // Extract comparison results and apply to bitmap
                ApplyDoubleMaskToBitmap(mask, ref selection, i, hasNulls, nullBitmap);
            }

            // Scalar tail
            for (; i < length; i++)
            {
                if (!selection[i]) continue;
                if (hasNulls && IsNull(nullBitmap, i))
                {
                    selection.Clear(i);
                    continue;
                }
                if (!EvaluateScalar(values[i]))
                {
                    selection.Clear(i);
                }
            }
        }
        else
        {
            // Scalar fallback for non-AVX2 systems
            for (int i = 0; i < length; i++)
            {
                if (!selection[i]) continue;
                if (hasNulls && IsNull(nullBitmap, i))
                {
                    selection.Clear(i);
                    continue;
                }
                if (!EvaluateScalar(values[i]))
                {
                    selection.Clear(i);
                }
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ApplyDoubleMaskToBitmap(Vector256<double> mask, ref SelectionBitmap selection, int startIndex, bool hasNulls, ReadOnlySpan<byte> nullBitmap)
    {
        // Extract each element and apply to bitmap (4 doubles)
        for (int j = 0; j < 4; j++)
        {
            var idx = startIndex + j;
            if (!selection[idx]) continue;
            
            if (hasNulls && IsNull(nullBitmap, idx))
            {
                selection.Clear(idx);
                continue;
            }
            
            // mask[j] is all 1s if true, all 0s if false (as a double bit pattern)
            if (BitConverter.DoubleToInt64Bits(mask.GetElement(j)) == 0)
            {
                selection.Clear(idx);
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsNull(ReadOnlySpan<byte> nullBitmap, int index)
    {
        if (nullBitmap.IsEmpty) return false;
        return (nullBitmap[index >> 3] & (1 << (index & 7))) == 0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool EvaluateScalar(double columnValue)
    {
        return Operator switch
        {
            ComparisonOperator.Equal => columnValue == Value,
            ComparisonOperator.NotEqual => columnValue != Value,
            ComparisonOperator.LessThan => columnValue < Value,
            ComparisonOperator.LessThanOrEqual => columnValue <= Value,
            ComparisonOperator.GreaterThan => columnValue > Value,
            ComparisonOperator.GreaterThanOrEqual => columnValue >= Value,
            _ => false
        };
    }

    protected override bool EvaluateSingle(IArrowArray column, int index)
    {
        if (column.IsNull(index)) return false;
        var columnValue = RunLengthEncodedArrayBuilder.GetDoubleValue(column, index);
        return EvaluateScalar(columnValue);
    }

    /// <summary>
    /// Evaluates this predicate for a range of rows with SIMD optimization.
    /// Thread-safe for non-overlapping ranges.
    /// </summary>
    public override void EvaluateRange(IArrowArray column, ref SelectionBitmap selection, int startIndex, int endIndex)
    {
        if (column is DoubleArray doubleArray)
        {
            EvaluateDoubleRangeSimd(doubleArray, ref selection, startIndex, endIndex);
            return;
        }

        // Fallback to scalar path
        base.EvaluateRange(column, ref selection, startIndex, endIndex);
    }

    private void EvaluateDoubleRangeSimd(DoubleArray array, ref SelectionBitmap selection, int startIndex, int endIndex)
    {
        var values = array.Values;
        var nullBitmap = array.NullBitmapBuffer.Span;
        var hasNulls = array.NullCount > 0;
        int i = startIndex;

        // SIMD path: process 4 elements at a time with AVX2 (256-bit = 4 doubles)
        if (Vector256.IsHardwareAccelerated && (endIndex - startIndex) >= 4)
        {
            var compareValue = Vector256.Create(Value);
            ref double valuesRef = ref Unsafe.AsRef(in values[0]);
            
            // Align to 4-element boundary from startIndex
            int vectorStart = ((startIndex + 3) >> 2) << 2; // Round up to next 4
            int vectorEnd = (endIndex >> 2) << 2; // Round down to 4

            // Scalar head (before aligned start)
            for (; i < vectorStart && i < endIndex; i++)
            {
                if (!selection[i]) continue;
                if (hasNulls && IsNull(nullBitmap, i))
                {
                    selection.Clear(i);
                    continue;
                }
                if (!EvaluateScalar(values[i]))
                {
                    selection.Clear(i);
                }
            }

            // SIMD middle
            for (; i < vectorEnd; i += 4)
            {
                var data = Vector256.LoadUnsafe(ref Unsafe.Add(ref valuesRef, i));
                
                Vector256<double> mask = Operator switch
                {
                    ComparisonOperator.Equal => Vector256.Equals(data, compareValue),
                    ComparisonOperator.NotEqual => ~Vector256.Equals(data, compareValue),
                    ComparisonOperator.LessThan => Vector256.LessThan(data, compareValue),
                    ComparisonOperator.LessThanOrEqual => Vector256.LessThanOrEqual(data, compareValue),
                    ComparisonOperator.GreaterThan => Vector256.GreaterThan(data, compareValue),
                    ComparisonOperator.GreaterThanOrEqual => Vector256.GreaterThanOrEqual(data, compareValue),
                    _ => Vector256<double>.Zero
                };

                ApplyDoubleMaskToBitmap(mask, ref selection, i, hasNulls, nullBitmap);
            }
        }

        // Scalar tail
        for (; i < endIndex; i++)
        {
            if (!selection[i]) continue;
            if (hasNulls && IsNull(nullBitmap, i))
            {
                selection.Clear(i);
                continue;
            }
            if (!EvaluateScalar(values[i]))
            {
                selection.Clear(i);
            }
        }
    }
}

/// <summary>
/// Predicate for comparing a Decimal column against a constant value.
/// </summary>
public sealed class DecimalComparisonPredicate : ColumnPredicate
{
    public override string ColumnName { get; }
    public ComparisonOperator Operator { get; }
    public decimal Value { get; }

    public DecimalComparisonPredicate(string columnName, ComparisonOperator op, decimal value)
    {
        ColumnName = columnName;
        Operator = op;
        Value = value;
    }

    public override void Evaluate(RecordBatch batch, Span<bool> selection)
    {
        var column = batch.Column(ColumnIndex);
        var length = batch.Length;

        for (int i = 0; i < length; i++)
        {
            if (!selection[i]) continue;

            if (column.IsNull(i))
            {
                selection[i] = false;
                continue;
            }

            var columnValue = RunLengthEncodedArrayBuilder.GetDecimalValue(column, i);
            selection[i] = Operator switch
            {
                ComparisonOperator.Equal => columnValue == Value,
                ComparisonOperator.NotEqual => columnValue != Value,
                ComparisonOperator.LessThan => columnValue < Value,
                ComparisonOperator.LessThanOrEqual => columnValue <= Value,
                ComparisonOperator.GreaterThan => columnValue > Value,
                ComparisonOperator.GreaterThanOrEqual => columnValue >= Value,
                _ => false
            };
        }
    }

    protected override bool EvaluateSingle(IArrowArray column, int index)
    {
        if (column.IsNull(index)) return false;
        var columnValue = RunLengthEncodedArrayBuilder.GetDecimalValue(column, index);
        return Operator switch
        {
            ComparisonOperator.Equal => columnValue == Value,
            ComparisonOperator.NotEqual => columnValue != Value,
            ComparisonOperator.LessThan => columnValue < Value,
            ComparisonOperator.LessThanOrEqual => columnValue <= Value,
            ComparisonOperator.GreaterThan => columnValue > Value,
            ComparisonOperator.GreaterThanOrEqual => columnValue >= Value,
            _ => false
        };
    }
}

/// <summary>
/// Predicate for string equality comparison.
/// </summary>
public sealed class StringEqualityPredicate : ColumnPredicate
{
    public override string ColumnName { get; }
    public string? Value { get; }
    public bool Negate { get; }
    public StringComparison Comparison { get; }

    public StringEqualityPredicate(string columnName, string? value, bool negate = false, StringComparison comparison = StringComparison.Ordinal)
    {
        ColumnName = columnName;
        Value = value;
        Negate = negate;
        Comparison = comparison;
    }

    public override void Evaluate(RecordBatch batch, Span<bool> selection)
    {
        var column = batch.Column(ColumnIndex);
        var length = batch.Length;

        for (int i = 0; i < length; i++)
        {
            if (!selection[i]) continue;

            if (column.IsNull(i))
            {
                // null == null is true, null == "something" is false
                var result = Value is null;
                selection[i] = Negate ? !result : result;
                continue;
            }

            if (Value is null)
            {
                // non-null != null
                selection[i] = Negate;
                continue;
            }

            var columnValue = RunLengthEncodedArrayBuilder.GetStringValue(column, i);
            var matches = string.Equals(columnValue, Value, Comparison);
            selection[i] = Negate ? !matches : matches;
        }
    }

    protected override bool EvaluateSingle(IArrowArray column, int index)
    {
        if (column.IsNull(index))
        {
            var result = Value is null;
            return Negate ? !result : result;
        }
        if (Value is null) return Negate;
        var columnValue = RunLengthEncodedArrayBuilder.GetStringValue(column, index);
        var matches = string.Equals(columnValue, Value, Comparison);
        return Negate ? !matches : matches;
    }
}

/// <summary>
/// Predicate for string operations like Contains, StartsWith, EndsWith.
/// </summary>
public sealed class StringOperationPredicate : ColumnPredicate
{
    public override string ColumnName { get; }
    public string Pattern { get; }
    public StringOperation Operation { get; }
    public StringComparison Comparison { get; }

    public StringOperationPredicate(string columnName, string pattern, StringOperation operation, StringComparison comparison = StringComparison.Ordinal)
    {
        ColumnName = columnName;
        Pattern = pattern ?? throw new ArgumentNullException(nameof(pattern));
        Operation = operation;
        Comparison = comparison;
    }

    public override void Evaluate(RecordBatch batch, Span<bool> selection)
    {
        var column = batch.Column(ColumnIndex);
        var length = batch.Length;

        for (int i = 0; i < length; i++)
        {
            if (!selection[i]) continue;

            if (column.IsNull(i))
            {
                selection[i] = false;
                continue;
            }

            var columnValue = RunLengthEncodedArrayBuilder.GetStringValue(column, i);
            selection[i] = Operation switch
            {
                StringOperation.Contains => columnValue.Contains(Pattern, Comparison),
                StringOperation.StartsWith => columnValue.StartsWith(Pattern, Comparison),
                StringOperation.EndsWith => columnValue.EndsWith(Pattern, Comparison),
                _ => false
            };
        }
    }

    protected override bool EvaluateSingle(IArrowArray column, int index)
    {
        if (column.IsNull(index)) return false;
        var columnValue = RunLengthEncodedArrayBuilder.GetStringValue(column, index);
        return Operation switch
        {
            StringOperation.Contains => columnValue.Contains(Pattern, Comparison),
            StringOperation.StartsWith => columnValue.StartsWith(Pattern, Comparison),
            StringOperation.EndsWith => columnValue.EndsWith(Pattern, Comparison),
            _ => false
        };
    }
}

/// <summary>
/// String operations supported in predicates.
/// </summary>
public enum StringOperation
{
    Contains,
    StartsWith,
    EndsWith
}

/// <summary>
/// Predicate for boolean column evaluation.
/// </summary>
public sealed class BooleanPredicate : ColumnPredicate
{
    public override string ColumnName { get; }
    public bool ExpectedValue { get; }

    public BooleanPredicate(string columnName, bool expectedValue = true)
    {
        ColumnName = columnName;
        ExpectedValue = expectedValue;
    }

    public override void Evaluate(RecordBatch batch, Span<bool> selection)
    {
        var column = batch.Column(ColumnIndex);
        var length = batch.Length;

        if (column is Apache.Arrow.BooleanArray boolArray)
        {
            for (int i = 0; i < length; i++)
            {
                if (!selection[i]) continue;

                if (boolArray.IsNull(i))
                {
                    selection[i] = false;
                    continue;
                }

                var value = boolArray.GetValue(i);
                selection[i] = value == ExpectedValue;
            }
        }
    }

    protected override bool EvaluateSingle(IArrowArray column, int index)
    {
        if (column is not Apache.Arrow.BooleanArray boolArray) return false;
        if (boolArray.IsNull(index)) return false;
        return boolArray.GetValue(index) == ExpectedValue;
    }
}

/// <summary>
/// Predicate for null checking.
/// </summary>
public sealed class IsNullPredicate : ColumnPredicate
{
    public override string ColumnName { get; }
    public bool CheckForNull { get; }

    public IsNullPredicate(string columnName, bool checkForNull = true)
    {
        ColumnName = columnName;
        CheckForNull = checkForNull;
    }

    public override void Evaluate(RecordBatch batch, Span<bool> selection)
    {
        var column = batch.Column(ColumnIndex);
        var length = batch.Length;

        for (int i = 0; i < length; i++)
        {
            if (!selection[i]) continue;

            var isNull = column.IsNull(i);
            selection[i] = CheckForNull ? isNull : !isNull;
        }
    }

    protected override bool EvaluateSingle(IArrowArray column, int index)
    {
        var isNull = column.IsNull(index);
        return CheckForNull ? isNull : !isNull;
    }
}

/// <summary>
/// A composite predicate that combines multiple predicates with AND logic.
/// </summary>
public sealed class AndPredicate : ColumnPredicate
{
    public override string ColumnName => string.Join(" AND ", _predicates.Select(p => p.ColumnName));
    
    private readonly List<ColumnPredicate> _predicates;

    public IReadOnlyList<ColumnPredicate> Predicates => _predicates;

    public AndPredicate(IEnumerable<ColumnPredicate> predicates)
    {
        _predicates = predicates.ToList();
    }

    public override void Evaluate(RecordBatch batch, Span<bool> selection)
    {
        foreach (var predicate in _predicates)
        {
            predicate.Evaluate(batch, selection);
        }
    }
}
