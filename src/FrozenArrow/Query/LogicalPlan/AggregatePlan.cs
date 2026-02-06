namespace FrozenArrow.Query.LogicalPlan;

/// <summary>
/// Represents a simple aggregate operation (SUM, AVG, MIN, MAX, COUNT)
/// without grouping - produces a single scalar result.
/// </summary>
public sealed class AggregatePlan : LogicalPlanNode
{
    /// <summary>
    /// Creates a new aggregate plan.
    /// </summary>
    /// <param name="input">The input plan to aggregate.</param>
    /// <param name="operation">The aggregation operation.</param>
    /// <param name="columnName">The column to aggregate (null for COUNT).</param>
    /// <param name="outputType">The CLR type of the result.</param>
    public AggregatePlan(
        LogicalPlanNode input,
        AggregationOperation operation,
        string? columnName,
        Type outputType)
    {
        Input = input ?? throw new ArgumentNullException(nameof(input));
        Operation = operation;
        ColumnName = columnName;
        OutputType = outputType ?? throw new ArgumentNullException(nameof(outputType));

        // Aggregate produces a single scalar result
        OutputSchema = new Dictionary<string, Type> 
        { 
            ["Value"] = outputType 
        };
    }

    /// <summary>
    /// Gets the input plan to aggregate.
    /// </summary>
    public LogicalPlanNode Input { get; }

    /// <summary>
    /// Gets the aggregation operation.
    /// </summary>
    public AggregationOperation Operation { get; }

    /// <summary>
    /// Gets the column to aggregate (null for COUNT).
    /// </summary>
    public string? ColumnName { get; }

    /// <summary>
    /// Gets the output type of the aggregation result.
    /// </summary>
    public Type OutputType { get; }

    public override string Description => 
        ColumnName != null 
            ? $"Aggregate({Operation}({ColumnName}))" 
            : $"Aggregate({Operation}())";

    // Aggregate always produces a single row
    public override long EstimatedRowCount => 1;

    public override IReadOnlyDictionary<string, Type> OutputSchema { get; }

    public override TResult Accept<TResult>(ILogicalPlanVisitor<TResult> visitor)
    {
        return visitor.Visit(this);
    }
}
