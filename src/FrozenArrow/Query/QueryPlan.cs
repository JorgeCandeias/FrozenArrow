namespace FrozenArrow.Query;

/// <summary>
/// Represents the analyzed execution plan for an ArrowQuery.
/// </summary>
public sealed class QueryPlan
{
    /// <summary>
    /// Gets whether all operations in the query can be executed using optimized column-only access.
    /// </summary>
    public bool IsFullyOptimized { get; init; }

    /// <summary>
    /// Gets the reason why the query is not fully optimized, if applicable.
    /// </summary>
    public string? UnsupportedReason { get; init; }

    /// <summary>
    /// Gets the list of columns that will be accessed during query execution.
    /// </summary>
    public IReadOnlyList<string> ColumnsAccessed { get; init; } = [];

    /// <summary>
    /// Gets the column predicates that can be pushed down to column-level filtering.
    /// </summary>
    public IReadOnlyList<ColumnPredicate> ColumnPredicates { get; init; } = [];

    /// <summary>
    /// Gets whether there is a fallback predicate that requires row materialization.
    /// </summary>
    public bool HasFallbackPredicate { get; init; }

    /// <summary>
    /// Gets the aggregations to perform (for GroupBy queries).
    /// </summary>
    public IReadOnlyList<AggregationDescriptor> Aggregations { get; init; } = [];

    /// <summary>
    /// Gets the grouping key column name (for GroupBy queries).
    /// </summary>
    public string? GroupByColumn { get; init; }

    /// <summary>
    /// Gets the CLR type of the group key (for GroupBy queries).
    /// </summary>
    public Type? GroupByKeyType { get; init; }

    /// <summary>
    /// Gets whether this is a grouped query (GroupBy followed by Select with aggregates).
    /// </summary>
    public bool IsGroupedQuery => GroupByColumn is not null && Aggregations.Count > 0;

    /// <summary>
    /// Gets the result property name for the group key (defaults to "Key" if not specified).
    /// </summary>
    public string GroupByKeyResultPropertyName { get; init; } = "Key";

    /// <summary>
    /// Gets the simple aggregate operation (for non-grouped aggregates like Sum, Average, Min, Max).
    /// </summary>
    public SimpleAggregateOperation? SimpleAggregate { get; init; }

    /// <summary>
    /// Gets whether this query ends with ToDictionary after GroupBy.
    /// </summary>
    public bool IsToDictionaryQuery { get; init; }

    /// <summary>
    /// Gets the single aggregation to use as the dictionary value (for ToDictionary queries).
    /// </summary>
    public AggregationDescriptor? ToDictionaryValueAggregation { get; init; }

    /// <summary>
    /// Gets the estimated selectivity of the filter (0.0 to 1.0).
    /// This is an estimate of what fraction of rows will pass the filter.
    /// </summary>
    public double EstimatedSelectivity { get; init; } = 1.0;

    /// <summary>
    /// Gets the number of elements to skip (for pagination).
    /// Null means no Skip operation.
    /// </summary>
    public int? Skip { get; init; }

    /// <summary>
    /// Gets the maximum number of elements to take (for pagination).
    /// Null means no Take operation (return all results).
    /// </summary>
    public int? Take { get; init; }

    /// <summary>
    /// Gets whether Take/Skip operations appear before predicates in the query.
    /// True: .Take(N).Where(...) - limit dataset first, then filter
    /// False: .Where(...).Take(N) - filter first, then limit
    /// This affects execution strategy for optimal performance.
    /// </summary>
    public bool PaginationBeforePredicates { get; init; }

    /// <summary>
    /// Creates a string representation of the query plan for debugging.
    /// </summary>
    public override string ToString()
    {
        var lines = new List<string>
        {
            $"Query Plan (Optimized: {IsFullyOptimized})"
        };

        if (!IsFullyOptimized && UnsupportedReason is not null)
        {
            lines.Add($"  Unsupported: {UnsupportedReason}");
        }

        if (ColumnsAccessed.Count > 0)
        {
            lines.Add($"  Columns: {string.Join(", ", ColumnsAccessed)}");
        }

        if (ColumnPredicates.Count > 0)
        {
            lines.Add($"  Predicates: {ColumnPredicates.Count} column-level filter(s)");
        }

        if (HasFallbackPredicate)
        {
            lines.Add("  Fallback: Row-level predicate (will materialize)");
        }

        if (GroupByColumn is not null)
        {
            lines.Add($"  GroupBy: {GroupByColumn}");
        }

        if (Aggregations.Count > 0)
        {
            lines.Add($"  Aggregations: {string.Join(", ", Aggregations.Select(a => $"{a.Operation}({a.ColumnName})"))}");
        }

        if (SimpleAggregate is not null)
        {
            var colDisplay = SimpleAggregate.ColumnName ?? "";
            lines.Add($"  Aggregate: {SimpleAggregate.Operation}({colDisplay}) [Column-Level]");
        }

        if (IsToDictionaryQuery && ToDictionaryValueAggregation is not null)
        {
            var colDisplay = ToDictionaryValueAggregation.ColumnName ?? "";
            lines.Add($"  ToDictionary: Key={GroupByColumn}, Value={ToDictionaryValueAggregation.Operation}({colDisplay})");
        }

        if (Skip.HasValue)
        {
            lines.Add($"  Skip: {Skip.Value}");
        }

        if (Take.HasValue)
        {
            lines.Add($"  Take: {Take.Value}");
        }

        lines.Add($"  Est. Selectivity: {EstimatedSelectivity:P0}");

        return string.Join(Environment.NewLine, lines);
    }
}

/// <summary>
/// Describes an aggregation operation to perform.
/// </summary>
public sealed class AggregationDescriptor
{
    /// <summary>
    /// Gets the aggregation operation type.
    /// </summary>
    public AggregationOperation Operation { get; init; }

    /// <summary>
    /// Gets the column name to aggregate (null for Count).
    /// </summary>
    public string? ColumnName { get; init; }

    /// <summary>
    /// Gets the result property name in the projected type.
    /// </summary>
    public string ResultPropertyName { get; init; } = string.Empty;
}

/// <summary>
/// Supported aggregation operations.
/// </summary>
public enum AggregationOperation
{
    Count,
    Sum,
    Average,
    Min,
    Max,
    LongCount
}

/// <summary>
/// Describes a simple (non-grouped) aggregate operation.
/// </summary>
public sealed class SimpleAggregateOperation
{
    /// <summary>
    /// Gets the aggregation operation type.
    /// </summary>
    public required AggregationOperation Operation { get; init; }

    /// <summary>
    /// Gets the column name to aggregate (null for Count without selector).
    /// </summary>
    public string? ColumnName { get; init; }

    /// <summary>
    /// Gets the expected result type.
    /// </summary>
    public required Type ResultType { get; init; }
}
