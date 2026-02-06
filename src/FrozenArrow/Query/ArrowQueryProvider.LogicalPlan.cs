using System.Linq.Expressions;
using FrozenArrow.Query.LogicalPlan;

namespace FrozenArrow.Query;

/// <summary>
/// Extension of ArrowQueryProvider to support logical plan execution.
/// This is the integration layer that bridges logical plans to the existing execution engine.
/// </summary>
public sealed partial class ArrowQueryProvider
{
    /// <summary>
    /// Executes a query using the logical plan architecture.
    /// This is the new execution path that translates LINQ → Logical Plan → Optimized Plan → Results.
    /// </summary>
    private TResult ExecuteWithLogicalPlan<TResult>(Expression expression)
    {
        // Step 1: Build schema for translator
        var schema = new Dictionary<string, Type>();
        foreach (var kvp in _columnIndexMap)
        {
            // Get column type from record batch
            var columnIndex = kvp.Value;
            if (columnIndex >= 0 && columnIndex < _recordBatch.ColumnCount)
            {
                var column = _recordBatch.Column(columnIndex);
                schema[kvp.Key] = GetClrTypeFromArrowType(column.Data.DataType);
            }
        }

        // Step 2: Translate LINQ expression to logical plan
        var translator = new LinqToLogicalPlanTranslator(
            _source,
            _elementType,
            schema,
            _columnIndexMap,
            _count);

        var logicalPlan = translator.Translate(expression);

        // Step 3: Optimize the logical plan
        var optimizer = new LogicalPlanOptimizer(_zoneMap);
        var optimizedPlan = optimizer.Optimize(logicalPlan);

        // Step 4: Execute the optimized plan (bridge to existing execution)
        return ExecuteLogicalPlan<TResult>(optimizedPlan, expression);
    }

    /// <summary>
    /// Executes an optimized logical plan by bridging to the existing execution infrastructure.
    /// This maintains compatibility while using the new plan representation.
    /// </summary>
    private TResult ExecuteLogicalPlan<TResult>(LogicalPlanNode plan, Expression expression)
    {
        // For now, convert logical plan back to QueryPlan and use existing execution
        // Future: Execute logical plans directly with a new executor
        var queryPlan = ConvertLogicalPlanToQueryPlan(plan);
        return ExecutePlan<TResult>(queryPlan, expression);
    }

    /// <summary>
    /// Converts a logical plan to the existing QueryPlan format.
    /// This is a bridge to maintain compatibility during the migration.
    /// </summary>
    private QueryPlan ConvertLogicalPlanToQueryPlan(LogicalPlanNode plan)
    {
        var predicates = new List<ColumnPredicate>();
        double selectivity = 1.0;
        int? skip = null;
        int? take = null;
        bool paginationBeforePredicates = false;

        // Walk the logical plan tree and extract components
        var current = plan;
        bool seenFilter = false;

        while (current is not null)
        {
            switch (current)
            {
                case ScanPlan:
                    // Reached the base - stop traversing
                    current = null;
                    break;

                case FilterPlan filter:
                    predicates.AddRange(filter.Predicates);
                    selectivity = filter.EstimatedSelectivity;
                    seenFilter = true;
                    current = filter.Input;
                    break;

                case LimitPlan limit:
                    if (!seenFilter)
                    {
                        paginationBeforePredicates = true;
                    }
                    take = limit.Count;
                    current = limit.Input;
                    break;

                case OffsetPlan offset:
                    if (!seenFilter)
                    {
                        paginationBeforePredicates = true;
                    }
                    skip = offset.Count;
                    current = offset.Input;
                    break;

                case ProjectPlan project:
                    // Projections are handled during materialization
                    current = project.Input;
                    break;

                case AggregatePlan aggregate:
                    // Continue walking the input to collect predicates, pagination, etc.
                    current = aggregate.Input;
                    
                    // Finish walking the tree to collect all predicates
                    while (current is not null)
                    {
                        switch (current)
                        {
                            case ScanPlan:
                                current = null;
                                break;
                                
                            case FilterPlan filter:
                                predicates.AddRange(filter.Predicates);
                                selectivity = filter.EstimatedSelectivity;
                                seenFilter = true;
                                current = filter.Input;
                                break;
                                
                            case LimitPlan limit:
                                if (!seenFilter)
                                {
                                    paginationBeforePredicates = true;
                                }
                                take = limit.Count;
                                current = limit.Input;
                                break;
                                
                            case OffsetPlan offset:
                                if (!seenFilter)
                                {
                                    paginationBeforePredicates = true;
                                }
                                skip = offset.Count;
                                current = offset.Input;
                                break;
                                
                            case ProjectPlan project:
                                current = project.Input;
                                break;
                                
                            default:
                                throw new NotSupportedException($"Unexpected plan node '{current.GetType().Name}' below Aggregate");
                        }
                    }
                    
                    // Create a simple aggregate QueryPlan with all collected predicates
                    // Note: Count doesn't need a column, but other aggregates do
                    return new QueryPlan
                    {
                        IsFullyOptimized = true,
                        ColumnPredicates = predicates,
                        EstimatedSelectivity = selectivity,
                        Skip = skip,
                        Take = take,
                        PaginationBeforePredicates = paginationBeforePredicates,
                        SimpleAggregate = aggregate.Operation == AggregationOperation.Count && aggregate.ColumnName == null
                            ? null // Count without column - handled as row count
                            : new SimpleAggregateOperation
                            {
                                Operation = aggregate.Operation,
                                ColumnName = aggregate.ColumnName,
                                ResultType = aggregate.OutputType
                            }
                    };

                case GroupByPlan groupBy:
                    // Continue walking the input to collect predicates, pagination, etc.
                    // GroupBy can have filters/pagination below it
                    current = groupBy.Input;
                    
                    // After collecting everything, create the grouped query QueryPlan
                    // We need to finish walking the tree first
                    while (current is not null)
                    {
                        switch (current)
                        {
                            case ScanPlan:
                                current = null;
                                break;
                                
                            case FilterPlan filter:
                                predicates.AddRange(filter.Predicates);
                                selectivity = filter.EstimatedSelectivity;
                                seenFilter = true;
                                current = filter.Input;
                                break;
                                
                            case LimitPlan limit:
                                if (!seenFilter)
                                {
                                    paginationBeforePredicates = true;
                                }
                                take = limit.Count;
                                current = limit.Input;
                                break;
                                
                            case OffsetPlan offset:
                                if (!seenFilter)
                                {
                                    paginationBeforePredicates = true;
                                }
                                skip = offset.Count;
                                current = offset.Input;
                                break;
                                
                            case ProjectPlan project:
                                // Projections are handled during materialization
                                current = project.Input;
                                break;
                                
                            default:
                                throw new NotSupportedException($"Unexpected plan node '{current.GetType().Name}' below GroupBy");
                        }
                    }
                    
                    // Now return the grouped query with all collected predicates
                    return new QueryPlan
                    {
                        IsFullyOptimized = true,
                        ColumnPredicates = predicates,
                        EstimatedSelectivity = selectivity,
                        Skip = skip,
                        Take = take,
                        PaginationBeforePredicates = paginationBeforePredicates,
                        GroupByColumn = groupBy.GroupByColumn,
                        GroupByKeyType = groupBy.GroupByKeyType,
                        GroupByKeyResultPropertyName = groupBy.KeyPropertyName ?? "Key",
                        Aggregations = [.. groupBy.Aggregations]
                    };

                default:
                    throw new NotSupportedException($"Logical plan node type '{current.GetType().Name}' is not yet supported for execution");
            }
        }

        // Simple query (just filtering and/or pagination)
        return new QueryPlan
        {
            IsFullyOptimized = true,
            ColumnPredicates = predicates,
            EstimatedSelectivity = selectivity,
            Skip = skip,
            Take = take,
            PaginationBeforePredicates = paginationBeforePredicates
        };
    }

    /// <summary>
    /// Gets the CLR type corresponding to an Arrow data type.
    /// </summary>
    private static Type GetClrTypeFromArrowType(Apache.Arrow.Types.IArrowType arrowType)
    {
        return arrowType switch
        {
            Apache.Arrow.Types.Int32Type => typeof(int),
            Apache.Arrow.Types.Int64Type => typeof(long),
            Apache.Arrow.Types.FloatType => typeof(float),
            Apache.Arrow.Types.DoubleType => typeof(double),
            Apache.Arrow.Types.StringType => typeof(string),
            Apache.Arrow.Types.BooleanType => typeof(bool),
            Apache.Arrow.Types.Date32Type => typeof(DateTime),
            Apache.Arrow.Types.Date64Type => typeof(DateTime),
            Apache.Arrow.Types.TimestampType => typeof(DateTimeOffset),
            _ => typeof(object)
        };
    }
}
