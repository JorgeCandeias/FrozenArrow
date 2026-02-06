namespace FrozenArrow.Query.LogicalPlan;

/// <summary>
/// Transforms logical plans to improve performance without changing semantics.
/// </summary>
/// <remarks>
/// This is where FrozenArrow's query optimizations live:
/// - Predicate reordering (most selective first)
/// - Filter pushdown
/// - Fused operations (filter + aggregate in one pass)
/// - Zone map utilization
/// </remarks>
public sealed class LogicalPlanOptimizer
{
    private readonly ZoneMap? _zoneMap;

    public LogicalPlanOptimizer(ZoneMap? zoneMap = null)
    {
        _zoneMap = zoneMap;
    }

    /// <summary>
    /// Optimizes a logical plan.
    /// Returns a new optimized plan (original is unchanged).
    /// </summary>
    public LogicalPlanNode Optimize(LogicalPlanNode plan)
    {
        // Apply optimization rules in order
        plan = OptimizePredicates(plan);
        plan = OptimizeFusedOperations(plan);
        // Future: OptimizePushdown, OptimizeJoins, etc.

        return plan;
    }

    /// <summary>
    /// Reorders predicates by estimated selectivity (most selective first).
    /// Uses zone map statistics when available.
    /// </summary>
    private LogicalPlanNode OptimizePredicates(LogicalPlanNode plan)
    {
        return plan.Accept(new PredicateReorderingVisitor(_zoneMap));
    }

    /// <summary>
    /// Identifies opportunities for fused operations (e.g., filter + aggregate).
    /// </summary>
    private LogicalPlanNode OptimizeFusedOperations(LogicalPlanNode plan)
    {
        // Pattern: Filter ? Aggregate can become FusedFilterAggregate
        if (plan is AggregatePlan agg && agg.Input is FilterPlan filter)
        {
            // Mark this pattern for fused execution (physical planner will handle)
            // For now, keep the logical plan unchanged
            // Physical planner will recognize this pattern and use FusedAggregator
        }

        return plan;
    }

    /// <summary>
    /// Visitor that reorders predicates in FilterPlan nodes.
    /// </summary>
    private sealed class PredicateReorderingVisitor : ILogicalPlanVisitor<LogicalPlanNode>
    {
        private readonly ZoneMap? _zoneMap;

        public PredicateReorderingVisitor(ZoneMap? zoneMap)
        {
            _zoneMap = zoneMap;
        }

        public LogicalPlanNode Visit(ScanPlan plan) => plan;

        public LogicalPlanNode Visit(FilterPlan plan)
        {
            // Recursively optimize input first
            var optimizedInput = plan.Input.Accept(this);

            // Reorder predicates by estimated selectivity
            var reorderedPredicates = PredicateReorderer.ReorderBySelectivity(
                plan.Predicates,
                _zoneMap,
                (int)plan.Input.EstimatedRowCount);

            // If predicates were reordered, create new FilterPlan
            if (!ReferenceEquals(reorderedPredicates, plan.Predicates) || 
                !ReferenceEquals(optimizedInput, plan.Input))
            {
                return new FilterPlan(optimizedInput, reorderedPredicates, plan.EstimatedSelectivity);
            }

            return plan;
        }

        public LogicalPlanNode Visit(ProjectPlan plan)
        {
            var optimizedInput = plan.Input.Accept(this);
            return ReferenceEquals(optimizedInput, plan.Input)
                ? plan
                : new ProjectPlan(optimizedInput, plan.Projections);
        }

        public LogicalPlanNode Visit(AggregatePlan plan)
        {
            var optimizedInput = plan.Input.Accept(this);
            return ReferenceEquals(optimizedInput, plan.Input)
                ? plan
                : new AggregatePlan(optimizedInput, plan.Operation, plan.ColumnName, plan.OutputType);
        }

        public LogicalPlanNode Visit(GroupByPlan plan)
        {
            var optimizedInput = plan.Input.Accept(this);
            return ReferenceEquals(optimizedInput, plan.Input)
                ? plan
                : new GroupByPlan(optimizedInput, plan.GroupByColumn, plan.GroupByKeyType, plan.Aggregations);
        }

        public LogicalPlanNode Visit(LimitPlan plan)
        {
            var optimizedInput = plan.Input.Accept(this);
            return ReferenceEquals(optimizedInput, plan.Input)
                ? plan
                : new LimitPlan(optimizedInput, plan.Count);
        }

        public LogicalPlanNode Visit(OffsetPlan plan)
        {
            var optimizedInput = plan.Input.Accept(this);
            return ReferenceEquals(optimizedInput, plan.Input)
                ? plan
                : new OffsetPlan(optimizedInput, plan.Count);
        }
    }
}
