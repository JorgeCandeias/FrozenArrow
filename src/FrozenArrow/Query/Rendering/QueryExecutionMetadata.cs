namespace FrozenArrow.Query.Rendering;

/// <summary>
/// Metadata about query execution for debugging and profiling.
/// </summary>
/// <remarks>
/// This is optional information that can be collected during query execution
/// to help diagnose performance issues and understand query behavior.
/// </remarks>
public sealed record QueryExecutionMetadata
{
    /// <summary>
    /// The type of plan that was executed (Logical, Physical, Compiled, etc.).
    /// </summary>
    public string? PlanType { get; init; }

    /// <summary>
    /// Whether zone maps were used for predicate elimination.
    /// </summary>
    public bool UsedZoneMaps { get; init; }

    /// <summary>
    /// Whether SIMD acceleration was used.
    /// </summary>
    public bool UsedSimd { get; init; }

    /// <summary>
    /// Whether parallel execution was used.
    /// </summary>
    public bool UsedParallelExecution { get; init; }

    /// <summary>
    /// The number of rows processed by the query engine.
    /// </summary>
    public int RowsProcessed { get; init; }

    /// <summary>
    /// The number of rows that passed all predicates.
    /// </summary>
    public int RowsSelected { get; init; }

    /// <summary>
    /// The number of predicates evaluated.
    /// </summary>
    public int PredicateCount { get; init; }

    /// <summary>
    /// Additional custom properties for extensibility.
    /// </summary>
    /// <remarks>
    /// Exposed as IReadOnlyDictionary to prevent mutation after construction.
    /// Use 'with' syntax to create modified copies if needed.
    /// </remarks>
    public IReadOnlyDictionary<string, object>? Properties { get; init; }

    /// <summary>
    /// Returns a string representation of the metadata.
    /// </summary>
    public override string ToString()
    {
        return $"QueryExecutionMetadata: Plan={PlanType}, ZoneMaps={UsedZoneMaps}, SIMD={UsedSimd}, " +
               $"Parallel={UsedParallelExecution}, Processed={RowsProcessed}, Selected={RowsSelected}";
    }
}
