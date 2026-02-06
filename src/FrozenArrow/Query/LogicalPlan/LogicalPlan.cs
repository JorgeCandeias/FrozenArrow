namespace FrozenArrow.Query.LogicalPlan;

/// <summary>
/// Base class for logical query plan nodes.
/// Logical plans represent WHAT to compute, independent of HOW it's executed.
/// They are immutable and can be transformed by query optimizers.
/// </summary>
/// <remarks>
/// Design principles:
/// - Immutable by default (thread-safe)
/// - API-agnostic (works with LINQ, SQL, JSON, etc.)
/// - Optimizable (can be transformed without breaking user-facing APIs)
/// - Serializable (can be cached, logged, visualized)
/// </remarks>
public abstract class LogicalPlanNode
{
    /// <summary>
    /// Gets a human-readable description of this plan node for debugging/visualization.
    /// </summary>
    public abstract string Description { get; }

    /// <summary>
    /// Gets the estimated number of rows this plan will produce.
    /// Used by optimizers for cost-based decisions.
    /// </summary>
    public abstract long EstimatedRowCount { get; }

    /// <summary>
    /// Gets the schema (column names and types) this plan produces.
    /// </summary>
    public abstract IReadOnlyDictionary<string, Type> OutputSchema { get; }

    /// <summary>
    /// Accepts a visitor for the visitor pattern (enables plan transformation).
    /// </summary>
    public abstract TResult Accept<TResult>(ILogicalPlanVisitor<TResult> visitor);
}

/// <summary>
/// Visitor interface for traversing and transforming logical plans.
/// </summary>
public interface ILogicalPlanVisitor<out TResult>
{
    TResult Visit(ScanPlan plan);
    TResult Visit(FilterPlan plan);
    TResult Visit(ProjectPlan plan);
    TResult Visit(AggregatePlan plan);
    TResult Visit(GroupByPlan plan);
    TResult Visit(LimitPlan plan);
    TResult Visit(OffsetPlan plan);
}
