namespace FrozenArrow.Query.LogicalPlan;

/// <summary>
/// Represents a GROUP BY operation with aggregations.
/// Groups rows by one or more keys and computes aggregates per group.
/// </summary>
public sealed class GroupByPlan : LogicalPlanNode
{
    /// <summary>
    /// Creates a new GROUP BY plan.
    /// </summary>
    /// <param name="input">The input plan to group.</param>
    /// <param name="groupByColumn">The column to group by.</param>
    /// <param name="groupByKeyType">The CLR type of the group key.</param>
    /// <param name="aggregations">The aggregations to compute per group.</param>
    public GroupByPlan(
        LogicalPlanNode input,
        string groupByColumn,
        Type groupByKeyType,
        IReadOnlyList<AggregationDescriptor> aggregations)
    {
        Input = input ?? throw new ArgumentNullException(nameof(input));
        GroupByColumn = groupByColumn ?? throw new ArgumentNullException(nameof(groupByColumn));
        GroupByKeyType = groupByKeyType ?? throw new ArgumentNullException(nameof(groupByKeyType));
        Aggregations = aggregations ?? throw new ArgumentNullException(nameof(aggregations));

        // Build output schema: Key + aggregation results
        var schema = new Dictionary<string, Type>
        {
            ["Key"] = groupByKeyType
        };
        foreach (var agg in aggregations)
        {
            schema[agg.ResultPropertyName] = GetAggregateResultType(agg);
        }
        OutputSchema = schema;
    }

    /// <summary>
    /// Gets the input plan to group.
    /// </summary>
    public LogicalPlanNode Input { get; }

    /// <summary>
    /// Gets the column name to group by.
    /// </summary>
    public string GroupByColumn { get; }

    /// <summary>
    /// Gets the CLR type of the group key.
    /// </summary>
    public Type GroupByKeyType { get; }

    /// <summary>
    /// Gets the aggregations to compute per group.
    /// </summary>
    public IReadOnlyList<AggregationDescriptor> Aggregations { get; }

    public override string Description => 
        $"GroupBy({GroupByColumn}) ? [{string.Join(", ", Aggregations.Select(a => $"{a.Operation}({a.ColumnName ?? ""})"))}]";

    // Estimated row count is the estimated number of unique groups
    // For now, use a simple heuristic: min(inputRows, inputRows * 0.1)
    public override long EstimatedRowCount => 
        Math.Min(Input.EstimatedRowCount, Math.Max(1, Input.EstimatedRowCount / 10));

    public override IReadOnlyDictionary<string, Type> OutputSchema { get; }

    public override TResult Accept<TResult>(ILogicalPlanVisitor<TResult> visitor)
    {
        return visitor.Visit(this);
    }

    private static Type GetAggregateResultType(AggregationDescriptor agg)
    {
        return agg.Operation switch
        {
            AggregationOperation.Count => typeof(long),
            AggregationOperation.Sum => typeof(long), // Simplified - would need column type info
            AggregationOperation.Average => typeof(double),
            AggregationOperation.Min => typeof(object), // Would need column type info
            AggregationOperation.Max => typeof(object), // Would need column type info
            _ => typeof(object)
        };
    }
}
