using Apache.Arrow;

namespace FrozenArrow.Query.PhysicalPlan;

/// <summary>
/// Executes physical plans by delegating to existing optimized executors.
/// Phase 6 Complete: Demonstrates strategy-based execution architecture.
/// </summary>
public sealed class PhysicalPlanExecutor
{
    private readonly RecordBatch _recordBatch;
    private readonly int _count;
    private readonly Dictionary<string, int> _columnIndexMap;
    private readonly Func<RecordBatch, int, object> _createItem;
    private readonly ZoneMap? _zoneMap;
    private readonly ParallelQueryOptions? _parallelOptions;

    public PhysicalPlanExecutor(
        RecordBatch recordBatch,
        int count,
        Dictionary<string, int> columnIndexMap,
        Func<RecordBatch, int, object> createItem,
        ZoneMap? zoneMap,
        ParallelQueryOptions? parallelOptions)
    {
        _recordBatch = recordBatch ?? throw new ArgumentNullException(nameof(recordBatch));
        _count = count;
        _columnIndexMap = columnIndexMap ?? throw new ArgumentNullException(nameof(columnIndexMap));
        _createItem = createItem ?? throw new ArgumentNullException(nameof(createItem));
        _zoneMap = zoneMap;
        _parallelOptions = parallelOptions;
    }

    /// <summary>
    /// Executes a physical plan and returns results.
    /// Delegates to existing optimized executors based on chosen strategy.
    /// </summary>
    public TResult Execute<TResult>(PhysicalPlanNode plan)
    {
        // For Phase 6, we delegate to existing executors
        // The key contribution is the cost-based strategy selection done by PhysicalPlanner
        // Future: Implement strategy-specific execution paths here

        // Convert physical plan back to logical for execution
        // This demonstrates the architecture - actual strategy-specific execution is future work
        var logicalPlan = ConvertToLogicalPlan(plan);
        
        // Use existing logical plan executor
        var logicalExecutor = new LogicalPlan.LogicalPlanExecutor(
            _recordBatch,
            _count,
            _columnIndexMap,
            _createItem,
            _zoneMap,
            _parallelOptions);

        return logicalExecutor.Execute<TResult>(logicalPlan);
    }

    /// <summary>
    /// Converts a physical plan back to a logical plan for execution.
    /// This is a temporary bridge - future work will implement direct physical execution.
    /// </summary>
    private LogicalPlan.LogicalPlanNode ConvertToLogicalPlan(PhysicalPlanNode physical)
    {
        return physical switch
        {
            PhysicalScanPlan scan => new LogicalPlan.ScanPlan(
                "physical",
                new object(),
                new Dictionary<string, Type>(),
                scan.RowCount),

            PhysicalFilterPlan filter => new LogicalPlan.FilterPlan(
                ConvertToLogicalPlan(filter.Input),
                filter.Predicates,
                filter.Selectivity),

            PhysicalGroupByPlan groupBy => new LogicalPlan.GroupByPlan(
                ConvertToLogicalPlan(groupBy.Input),
                groupBy.GroupByColumn,
                groupBy.GroupByKeyType,
                groupBy.Aggregations,
                groupBy.KeyPropertyName),

            PhysicalAggregatePlan aggregate => new LogicalPlan.AggregatePlan(
                ConvertToLogicalPlan(aggregate.Input),
                aggregate.Operation,
                aggregate.ColumnName,
                aggregate.OutputType),

            PhysicalLimitPlan limit => new LogicalPlan.LimitPlan(
                ConvertToLogicalPlan(limit.Input),
                limit.Count),

            PhysicalOffsetPlan offset => new LogicalPlan.OffsetPlan(
                ConvertToLogicalPlan(offset.Input),
                offset.Count),

            _ => throw new NotSupportedException($"Physical plan type '{physical.GetType().Name}' cannot be converted to logical plan")
        };
    }
}
