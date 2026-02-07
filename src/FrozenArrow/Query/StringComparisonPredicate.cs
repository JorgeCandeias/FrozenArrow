using Apache.Arrow;
using Apache.Arrow.Types;

namespace FrozenArrow.Query;

/// <summary>
/// Predicate for string column comparisons.
/// Phase 8 Enhancement: Enables SQL string predicates and LIKE operator.
/// Supports equality, LIKE patterns (%, _), and case-insensitive comparisons.
/// </summary>
public sealed class StringComparisonPredicate : ColumnPredicate
{
    public override string ColumnName { get; }
    public override int ColumnIndex { get; }
    public StringComparisonOperator Operator { get; }
    public string Value { get; }
    
    private readonly StringComparison _comparisonType;

    public StringComparisonPredicate(
        string columnName,
        int columnIndex,
        StringComparisonOperator op,
        string value,
        bool ignoreCase = false)
    {
        ColumnName = columnName;
        ColumnIndex = columnIndex;
        Operator = op;
        Value = value ?? throw new ArgumentNullException(nameof(value));
        _comparisonType = ignoreCase ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
    }

    public override void Evaluate(RecordBatch batch, Span<bool> selection)
    {
        var column = batch.Column(ColumnIndex);
        var length = batch.Length;

        for (int i = 0; i < length; i++)
        {
            if (!selection[i]) continue; // Already filtered out

            // Handle nulls
            if (column.IsNull(i))
            {
                selection[i] = false;
                continue;
            }

            // Get string value
            var stringValue = GetStringValue(column, i);
            
            if (stringValue == null)
            {
                selection[i] = false;
                continue;
            }

            // Perform comparison
            selection[i] = EvaluateString(stringValue);
        }
    }

    /// <summary>
    /// Evaluates a single row. Required by base class for bitmap operations.
    /// </summary>
    protected override bool EvaluateSingle(IArrowArray column, int rowIndex)
    {
        // Handle nulls
        if (column.IsNull(rowIndex))
        {
            return false;
        }

        // Get string value
        var stringValue = GetStringValue(column, rowIndex);
        
        if (stringValue == null)
        {
            return false;
        }

        // Perform comparison
        return EvaluateString(stringValue);
    }

    private string? GetStringValue(IArrowArray column, int rowIndex)
    {
        // Handle StringArray directly
        if (column is StringArray stringArray)
        {
            return stringArray.GetString(rowIndex);
        }

        // Handle DictionaryArray (encoded strings)
        if (column is DictionaryArray dictArray)
        {
            var valueArray = dictArray.Dictionary;
            if (valueArray is StringArray dictStrings)
            {
                var indices = dictArray.Indices;
                
                // Get the index - handle different index types
                int index;
                if (indices is Int8Array int8Indices)
                {
                    index = int8Indices.GetValue(rowIndex) ?? -1;
                }
                else if (indices is Int16Array int16Indices)
                {
                    index = int16Indices.GetValue(rowIndex) ?? -1;
                }
                else if (indices is Int32Array int32Indices)
                {
                    index = int32Indices.GetValue(rowIndex) ?? -1;
                }
                else
                {
                    return null;
                }

                if (index >= 0 && index < dictStrings.Length)
                {
                    return dictStrings.GetString(index);
                }
            }
        }

        return null;
    }

    private bool EvaluateString(string stringValue)
    {
        return Operator switch
        {
            StringComparisonOperator.Equal =>
                string.Equals(stringValue, Value, _comparisonType),

            StringComparisonOperator.NotEqual =>
                !string.Equals(stringValue, Value, _comparisonType),

            StringComparisonOperator.StartsWith =>
                stringValue.StartsWith(Value, _comparisonType),

            StringComparisonOperator.EndsWith =>
                stringValue.EndsWith(Value, _comparisonType),

            StringComparisonOperator.Contains =>
                stringValue.Contains(Value, _comparisonType),

            StringComparisonOperator.EqualIgnoreCase =>
                string.Equals(stringValue, Value, StringComparison.OrdinalIgnoreCase),

            StringComparisonOperator.GreaterThan =>
                string.Compare(stringValue, Value, _comparisonType) > 0,

            StringComparisonOperator.LessThan =>
                string.Compare(stringValue, Value, _comparisonType) < 0,

            StringComparisonOperator.GreaterThanOrEqual =>
                string.Compare(stringValue, Value, _comparisonType) >= 0,

            StringComparisonOperator.LessThanOrEqual =>
                string.Compare(stringValue, Value, _comparisonType) <= 0,

            _ => throw new NotSupportedException($"String operator {Operator} not supported")
        };
    }

    public override string ToString()
    {
        var opStr = Operator switch
        {
            StringComparisonOperator.Equal => "=",
            StringComparisonOperator.NotEqual => "!=",
            StringComparisonOperator.StartsWith => "LIKE prefix",
            StringComparisonOperator.EndsWith => "LIKE suffix",
            StringComparisonOperator.Contains => "LIKE contains",
            StringComparisonOperator.GreaterThan => ">",
            StringComparisonOperator.LessThan => "<",
            StringComparisonOperator.GreaterThanOrEqual => ">=",
            StringComparisonOperator.LessThanOrEqual => "<=",
            _ => Operator.ToString()
        };

        return $"{ColumnName} {opStr} '{Value}'";
    }
}

