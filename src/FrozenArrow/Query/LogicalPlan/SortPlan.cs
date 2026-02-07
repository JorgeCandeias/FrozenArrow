namespace FrozenArrow.Query.LogicalPlan;

/// <summary>
/// Represents sorting direction for ORDER BY.
/// Phase B: ORDER BY support.
/// </summary>
public enum SortDirection
{
    /// <summary>
    /// Ascending order (default)
    /// </summary>
    Ascending,

    /// <summary>
    /// Descending order
    /// </summary>
    Descending
}

/// <summary>
/// Represents a single sort specification in ORDER BY.
/// </summary>
public sealed class SortSpecification
{
    public string ColumnName { get; }
    public SortDirection Direction { get; }

    public SortSpecification(string columnName, SortDirection direction = SortDirection.Ascending)
    {
        ColumnName = columnName ?? throw new ArgumentNullException(nameof(columnName));
        Direction = direction;
    }

    public override string ToString()
    {
        return Direction == SortDirection.Ascending 
            ? ColumnName 
            : $"{ColumnName} DESC";
    }
}

/// <summary>
/// Logical plan node representing an ORDER BY operation (sorting).
/// Phase B: ORDER BY support.
/// </summary>
public sealed class SortPlan : LogicalPlanNode
{
    public LogicalPlanNode Input { get; }
    public IReadOnlyList<SortSpecification> SortSpecifications { get; }

    public SortPlan(LogicalPlanNode input, IReadOnlyList<SortSpecification> sortSpecifications)
    {
        Input = input ?? throw new ArgumentNullException(nameof(input));
        SortSpecifications = sortSpecifications ?? throw new ArgumentNullException(nameof(sortSpecifications));
        
        if (sortSpecifications.Count == 0)
            throw new ArgumentException("At least one sort specification is required", nameof(sortSpecifications));
    }

    /// <summary>
    /// Convenience constructor for single-column sort.
    /// </summary>
    public SortPlan(LogicalPlanNode input, string columnName, SortDirection direction = SortDirection.Ascending)
        : this(input, new[] { new SortSpecification(columnName, direction) })
    {
    }

    public override string Description => "Sort";

    public override long EstimatedRowCount => Input.EstimatedRowCount;

    public override IReadOnlyDictionary<string, Type> OutputSchema => Input.OutputSchema;

    public override TResult Accept<TResult>(ILogicalPlanVisitor<TResult> visitor)
    {
        // SortPlan doesn't need special optimization
        // Just return the plan as-is
        return (TResult)(object)this;
    }

    public override string ToString()
    {
        var sorts = string.Join(", ", SortSpecifications);
        return $"Sort({sorts})";
    }
}
