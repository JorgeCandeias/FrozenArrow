using Apache.Arrow;

namespace ArrowCollection.Query;

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
        // Default implementation: iterate and update bitmap
        // Subclasses can override for better performance
        var length = batch.Length;
        var column = batch.Column(ColumnIndex);

        for (int i = 0; i < length; i++)
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
        // Default: not implemented, subclasses should override Evaluate(ref SelectionBitmap) instead
        throw new NotImplementedException("Subclass must override either EvaluateSingle or Evaluate(ref SelectionBitmap)");
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

    protected override bool EvaluateSingle(IArrowArray column, int index)
    {
        if (column.IsNull(index)) return false;
        var columnValue = RunLengthEncodedArrayBuilder.GetInt32Value(column, index);
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
/// Predicate for comparing a Double column against a constant value.
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

    protected override bool EvaluateSingle(IArrowArray column, int index)
    {
        if (column.IsNull(index)) return false;
        var columnValue = RunLengthEncodedArrayBuilder.GetDoubleValue(column, index);
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
