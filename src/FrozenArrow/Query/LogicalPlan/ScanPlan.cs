namespace FrozenArrow.Query.LogicalPlan;

/// <summary>
/// Represents a table scan - the source of data for a query.
/// This is always the leaf node in a logical plan tree.
/// </summary>
public sealed class ScanPlan : LogicalPlanNode
{
    /// <summary>
    /// Creates a new scan plan.
    /// </summary>
    /// <param name="tableName">Logical name of the table (for debugging/caching).</param>
    /// <param name="sourceReference">Opaque reference to the actual data source (e.g., FrozenArrow{T}).</param>
    /// <param name="schema">Column names and their CLR types.</param>
    /// <param name="rowCount">Total number of rows in the source.</param>
    public ScanPlan(
        string tableName,
        object sourceReference,
        IReadOnlyDictionary<string, Type> schema,
        long rowCount)
    {
        TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
        SourceReference = sourceReference ?? throw new ArgumentNullException(nameof(sourceReference));
        OutputSchema = schema ?? throw new ArgumentNullException(nameof(schema));
        EstimatedRowCount = rowCount;
    }

    /// <summary>
    /// Gets the logical table name (used for plan visualization and caching).
    /// </summary>
    public string TableName { get; }

    /// <summary>
    /// Gets an opaque reference to the actual data source.
    /// The physical executor knows how to interpret this (e.g., as FrozenArrow{T}).
    /// </summary>
    public object SourceReference { get; }

    public override string Description => $"Scan({TableName})";

    public override long EstimatedRowCount { get; }

    public override IReadOnlyDictionary<string, Type> OutputSchema { get; }

    public override TResult Accept<TResult>(ILogicalPlanVisitor<TResult> visitor)
    {
        return visitor.Visit(this);
    }
}
