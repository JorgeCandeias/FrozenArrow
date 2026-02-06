namespace FrozenArrow.Query.LogicalPlan;

/// <summary>
/// Represents a LIMIT operation (Take in LINQ).
/// Limits the number of rows returned.
/// </summary>
public sealed class LimitPlan : LogicalPlanNode
{
    /// <summary>
    /// Creates a new LIMIT plan.
    /// </summary>
    /// <param name="input">The input plan to limit.</param>
    /// <param name="count">The maximum number of rows to return.</param>
    public LimitPlan(LogicalPlanNode input, int count)
    {
        Input = input ?? throw new ArgumentNullException(nameof(input));
        
        if (count < 0)
            throw new ArgumentOutOfRangeException(nameof(count), "Count must be non-negative");
        
        Count = count;
    }

    /// <summary>
    /// Gets the input plan to limit.
    /// </summary>
    public LogicalPlanNode Input { get; }

    /// <summary>
    /// Gets the maximum number of rows to return.
    /// </summary>
    public int Count { get; }

    public override string Description => $"Limit({Count})";

    public override long EstimatedRowCount => Math.Min(Input.EstimatedRowCount, Count);

    public override IReadOnlyDictionary<string, Type> OutputSchema => Input.OutputSchema;

    public override TResult Accept<TResult>(ILogicalPlanVisitor<TResult> visitor)
    {
        return visitor.Visit(this);
    }
}

/// <summary>
/// Represents an OFFSET operation (Skip in LINQ).
/// Skips a number of rows before returning results.
/// </summary>
public sealed class OffsetPlan : LogicalPlanNode
{
    /// <summary>
    /// Creates a new OFFSET plan.
    /// </summary>
    /// <param name="input">The input plan to skip from.</param>
    /// <param name="count">The number of rows to skip.</param>
    public OffsetPlan(LogicalPlanNode input, int count)
    {
        Input = input ?? throw new ArgumentNullException(nameof(input));
        
        if (count < 0)
            throw new ArgumentOutOfRangeException(nameof(count), "Count must be non-negative");
        
        Count = count;
    }

    /// <summary>
    /// Gets the input plan to skip from.
    /// </summary>
    public LogicalPlanNode Input { get; }

    /// <summary>
    /// Gets the number of rows to skip.
    /// </summary>
    public int Count { get; }

    public override string Description => $"Offset({Count})";

    public override long EstimatedRowCount => Math.Max(0, Input.EstimatedRowCount - Count);

    public override IReadOnlyDictionary<string, Type> OutputSchema => Input.OutputSchema;

    public override TResult Accept<TResult>(ILogicalPlanVisitor<TResult> visitor)
    {
        return visitor.Visit(this);
    }
}
