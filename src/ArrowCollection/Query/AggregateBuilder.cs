namespace ArrowCollection.Query;

/// <summary>
/// Builder for defining multiple aggregations to compute in a single pass.
/// Used with the Aggregate extension method.
/// </summary>
/// <typeparam name="T">The element type being aggregated.</typeparam>
public sealed class AggregateBuilder<T>
{
    internal List<AggregationDescriptor> Aggregations { get; } = [];

    /// <summary>
    /// Computes the count of selected elements.
    /// </summary>
    public int Count()
    {
        Aggregations.Add(new AggregationDescriptor
        {
            Operation = AggregationOperation.Count,
            ColumnName = null,
            ResultPropertyName = GetNextPlaceholder()
        });
        return default;
    }

    /// <summary>
    /// Computes the long count of selected elements.
    /// </summary>
    public long LongCount()
    {
        Aggregations.Add(new AggregationDescriptor
        {
            Operation = AggregationOperation.LongCount,
            ColumnName = null,
            ResultPropertyName = GetNextPlaceholder()
        });
        return default;
    }

    /// <summary>
    /// Computes the sum of the specified column.
    /// </summary>
    public TResult Sum<TResult>(System.Linq.Expressions.Expression<Func<T, TResult>> selector)
    {
        var columnName = ExtractColumnName(selector);
        Aggregations.Add(new AggregationDescriptor
        {
            Operation = AggregationOperation.Sum,
            ColumnName = columnName,
            ResultPropertyName = GetNextPlaceholder()
        });
        return default!;
    }

    /// <summary>
    /// Computes the average of the specified column.
    /// </summary>
    public double Average<TValue>(System.Linq.Expressions.Expression<Func<T, TValue>> selector)
    {
        var columnName = ExtractColumnName(selector);
        Aggregations.Add(new AggregationDescriptor
        {
            Operation = AggregationOperation.Average,
            ColumnName = columnName,
            ResultPropertyName = GetNextPlaceholder()
        });
        return default;
    }

    /// <summary>
    /// Computes the minimum value of the specified column.
    /// </summary>
    public TResult Min<TResult>(System.Linq.Expressions.Expression<Func<T, TResult>> selector)
    {
        var columnName = ExtractColumnName(selector);
        Aggregations.Add(new AggregationDescriptor
        {
            Operation = AggregationOperation.Min,
            ColumnName = columnName,
            ResultPropertyName = GetNextPlaceholder()
        });
        return default!;
    }

    /// <summary>
    /// Computes the maximum value of the specified column.
    /// </summary>
    public TResult Max<TResult>(System.Linq.Expressions.Expression<Func<T, TResult>> selector)
    {
        var columnName = ExtractColumnName(selector);
        Aggregations.Add(new AggregationDescriptor
        {
            Operation = AggregationOperation.Max,
            ColumnName = columnName,
            ResultPropertyName = GetNextPlaceholder()
        });
        return default!;
    }

    private int _placeholderIndex;

    private string GetNextPlaceholder() => $"__agg_{_placeholderIndex++}";

    private static string ExtractColumnName<TValue>(System.Linq.Expressions.Expression<Func<T, TValue>> selector)
    {
        if (selector.Body is System.Linq.Expressions.MemberExpression memberExpr)
        {
            return memberExpr.Member.Name;
        }
        
        if (selector.Body is System.Linq.Expressions.UnaryExpression unary && 
            unary.Operand is System.Linq.Expressions.MemberExpression innerMember)
        {
            return innerMember.Member.Name;
        }

        throw new ArgumentException("Selector must be a simple property access (x => x.Property).", nameof(selector));
    }
}
