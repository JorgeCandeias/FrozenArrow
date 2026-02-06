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
            var columnIndex = kvp.Value;
            if (columnIndex >= 0 && columnIndex < _recordBatch.ColumnCount)
            {
                var column = _recordBatch.Column(columnIndex);
                schema[kvp.Key] = GetClrTypeFromArrowType(column.Data.DataType);
            }
        }

        LogicalPlanNode optimizedPlan;

        // Step 2: Check cache if enabled (Phase 7)
        if (UseLogicalPlanCache)
        {
            var cacheKey = LogicalPlan.LogicalPlanCache.ComputeKey(expression.ToString());
            
            if (_logicalPlanCache.TryGet(cacheKey, out var cachedPlan))
            {
                // Cache hit!
                optimizedPlan = cachedPlan;
            }
            else
            {
                // Cache miss - translate and optimize
                optimizedPlan = TranslateAndOptimize(expression, schema);
                _logicalPlanCache.Add(cacheKey, optimizedPlan);
            }
        }
        else
        {
            // No caching
            optimizedPlan = TranslateAndOptimize(expression, schema);
        }

        // Step 3: Execute
        if (UsePhysicalPlanExecution)
        {
            return ExecuteLogicalPlanViaPhysical<TResult>(optimizedPlan);
        }
        else if (UseDirectLogicalPlanExecution)
        {
            return ExecuteLogicalPlanDirect<TResult>(optimizedPlan);
        }
        else
        {
            return ExecuteLogicalPlanViaBridge<TResult>(optimizedPlan, expression);
        }
    }

    private LogicalPlanNode TranslateAndOptimize(Expression expression, Dictionary<string, Type> schema)
    {
        var translator = new LinqToLogicalPlanTranslator(
            _source,
            _elementType,
            schema,
            _columnIndexMap,
            _count);

        var logicalPlan = translator.Translate(expression);

        var optimizer = new LogicalPlanOptimizer(_zoneMap);
        return optimizer.Optimize(logicalPlan);
    }

    /// <summary>
    /// Executes a logical plan via physical plan execution (Phase 6).
    /// Converts logical plan to physical plan with cost-based strategy selection,
    /// then executes with the physical executor.
    /// </summary>
    private TResult ExecuteLogicalPlanViaPhysical<TResult>(LogicalPlanNode logicalPlan)
    {
        try
        {
            // Convert logical plan to physical plan
            var planner = new PhysicalPlan.PhysicalPlanner();
            var physicalPlan = planner.CreatePhysicalPlan(logicalPlan);

            // Execute physical plan
            var executor = new PhysicalPlan.PhysicalPlanExecutor(
                _recordBatch,
                _count,
                _columnIndexMap,
                _createItem,
                _zoneMap,
                ParallelOptions);

            return executor.Execute<TResult>(physicalPlan);
        }
        catch (Exception)
        {
            // Fall back to direct logical plan execution on any error
            return ExecuteLogicalPlanDirect<TResult>(logicalPlan);
        }
    }

    /// <summary>
    /// Executes a logical plan directly without converting to QueryPlan (Phase 5).
    /// Falls back to bridge on any errors for stability.
    /// </summary>
    private TResult ExecuteLogicalPlanDirect<TResult>(LogicalPlanNode plan)
    {
        try
        {
            var executor = new LogicalPlanExecutor(
                _recordBatch,
                _count,
                _columnIndexMap,
                _createItem,
                _zoneMap,
                ParallelOptions);

            return executor.Execute<TResult>(plan);
        }
        catch (Exception)
        {
            // Fall back to bridge on any error
            // This ensures stability while we're perfecting direct execution
            return ExecuteLogicalPlanViaBridge<TResult>(plan, Expression.Constant(null));
        }
    }

    /// <summary>
    /// Executes an optimized logical plan by bridging to the existing execution infrastructure (Phase 3-4).
    /// This maintains compatibility while using the new plan representation.
    /// </summary>
    private TResult ExecuteLogicalPlanViaBridge<TResult>(LogicalPlanNode plan, Expression expression)
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
