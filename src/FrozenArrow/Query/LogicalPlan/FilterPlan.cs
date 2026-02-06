namespace FrozenArrow.Query.LogicalPlan;

/// <summary>
/// Represents a filter operation (WHERE clause).
/// Filters rows based on one or more predicates.
/// </summary>
public sealed class FilterPlan : LogicalPlanNode
{
    /// <summary>
    /// Creates a new filter plan.
    /// </summary>
    /// <param name="input">The input plan to filter.</param>
    /// <param name="predicates">Column-level predicates to evaluate.</param>
    /// <param name="estimatedSelectivity">Estimated fraction of rows that pass (0.0 to 1.0).</param>
    public FilterPlan(
        LogicalPlanNode input,
        IReadOnlyList<ColumnPredicate> predicates,
        double estimatedSelectivity = 0.5)
    {
        Input = input ?? throw new ArgumentNullException(nameof(input));
        Predicates = predicates ?? throw new ArgumentNullException(nameof(predicates));
        
        if (estimatedSelectivity < 0.0 || estimatedSelectivity > 1.0)
            throw new ArgumentOutOfRangeException(nameof(estimatedSelectivity));
        
        EstimatedSelectivity = estimatedSelectivity;
    }

    /// <summary>
    /// Gets the input plan to filter.
    /// </summary>
    public LogicalPlanNode Input { get; }

    /// <summary>
    /// Gets the predicates to evaluate.
    /// These are column-level predicates that can be pushed down to Arrow arrays.
    /// </summary>
    public IReadOnlyList<ColumnPredicate> Predicates { get; }

    /// <summary>
    /// Gets the estimated fraction of rows that will pass the filter (0.0 to 1.0).
    /// Used by optimizers for cost estimation and predicate reordering.
    /// </summary>
    public double EstimatedSelectivity { get; }

    public override string Description => 
        $"Filter({Predicates.Count} predicates, est. selectivity={EstimatedSelectivity:P0})";

    public override long EstimatedRowCount => 
        (long)(Input.EstimatedRowCount * EstimatedSelectivity);

    public override IReadOnlyDictionary<string, Type> OutputSchema => Input.OutputSchema;

    public override TResult Accept<TResult>(ILogicalPlanVisitor<TResult> visitor)
    {
        return visitor.Visit(this);
    }
}
