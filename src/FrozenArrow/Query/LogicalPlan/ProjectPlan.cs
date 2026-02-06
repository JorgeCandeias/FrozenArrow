namespace FrozenArrow.Query.LogicalPlan;

/// <summary>
/// Represents a projection operation (SELECT clause).
/// Selects specific columns or computes derived columns.
/// </summary>
public sealed class ProjectPlan : LogicalPlanNode
{
    /// <summary>
    /// Creates a new project plan.
    /// </summary>
    /// <param name="input">The input plan to project from.</param>
    /// <param name="projections">The columns to project.</param>
    public ProjectPlan(
        LogicalPlanNode input,
        IReadOnlyList<ProjectionColumn> projections)
    {
        Input = input ?? throw new ArgumentNullException(nameof(input));
        Projections = projections ?? throw new ArgumentNullException(nameof(projections));

        // Build output schema from projections
        var schema = new Dictionary<string, Type>(projections.Count);
        foreach (var proj in projections)
        {
            schema[proj.OutputName] = proj.OutputType;
        }
        OutputSchema = schema;
    }

    /// <summary>
    /// Gets the input plan to project from.
    /// </summary>
    public LogicalPlanNode Input { get; }

    /// <summary>
    /// Gets the columns to project (select).
    /// </summary>
    public IReadOnlyList<ProjectionColumn> Projections { get; }

    public override string Description => 
        $"Project({string.Join(", ", Projections.Select(p => p.OutputName))})";

    public override long EstimatedRowCount => Input.EstimatedRowCount;

    public override IReadOnlyDictionary<string, Type> OutputSchema { get; }

    public override TResult Accept<TResult>(ILogicalPlanVisitor<TResult> visitor)
    {
        return visitor.Visit(this);
    }
}

/// <summary>
/// Represents a single column in a projection.
/// </summary>
public sealed class ProjectionColumn
{
    /// <summary>
    /// Creates a simple column projection (pass-through).
    /// </summary>
    public ProjectionColumn(string sourceColumn, string outputName, Type outputType)
    {
        SourceColumn = sourceColumn ?? throw new ArgumentNullException(nameof(sourceColumn));
        OutputName = outputName ?? throw new ArgumentNullException(nameof(outputName));
        OutputType = outputType ?? throw new ArgumentNullException(nameof(outputType));
        Kind = ProjectionKind.Column;
    }

    /// <summary>
    /// Creates an aggregate projection (used in GroupBy results).
    /// </summary>
    public ProjectionColumn(
        string outputName, 
        Type outputType, 
        AggregationOperation aggregateOp,
        string? aggregateColumn = null)
    {
        OutputName = outputName ?? throw new ArgumentNullException(nameof(outputName));
        OutputType = outputType ?? throw new ArgumentNullException(nameof(outputType));
        Kind = ProjectionKind.Aggregate;
        AggregateOperation = aggregateOp;
        AggregateColumn = aggregateColumn;
    }

    /// <summary>
    /// Gets the kind of projection.
    /// </summary>
    public ProjectionKind Kind { get; }

    /// <summary>
    /// Gets the source column name (for column projections).
    /// </summary>
    public string? SourceColumn { get; }

    /// <summary>
    /// Gets the output column name.
    /// </summary>
    public string OutputName { get; }

    /// <summary>
    /// Gets the output column type.
    /// </summary>
    public Type OutputType { get; }

    /// <summary>
    /// Gets the aggregation operation (for aggregate projections).
    /// </summary>
    public AggregationOperation? AggregateOperation { get; }

    /// <summary>
    /// Gets the column to aggregate (null for COUNT).
    /// </summary>
    public string? AggregateColumn { get; }
}

/// <summary>
/// Kind of projection operation.
/// </summary>
public enum ProjectionKind
{
    /// <summary>
    /// Direct column reference (pass-through).
    /// </summary>
    Column,

    /// <summary>
    /// Aggregate function result.
    /// </summary>
    Aggregate,

    /// <summary>
    /// Computed expression (for future extension).
    /// </summary>
    Expression
}
