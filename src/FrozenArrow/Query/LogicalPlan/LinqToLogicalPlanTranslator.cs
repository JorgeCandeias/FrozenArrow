using System.Linq.Expressions;

namespace FrozenArrow.Query.LogicalPlan;

/// <summary>
/// Translates LINQ Expression trees to logical plans.
/// This is a thin adapter layer that decouples LINQ from the query engine.
/// </summary>
public sealed class LinqToLogicalPlanTranslator(
    object source,
    Type elementType,
    IReadOnlyDictionary<string, Type> schema,
    Dictionary<string, int> columnIndexMap,
    long rowCount)
{
    private readonly object _source = source ?? throw new ArgumentNullException(nameof(source));
    private readonly Type _elementType = elementType ?? throw new ArgumentNullException(nameof(elementType));
    private readonly IReadOnlyDictionary<string, Type> _schema = schema ?? throw new ArgumentNullException(nameof(schema));
    private readonly Dictionary<string, int> _columnIndexMap = columnIndexMap ?? throw new ArgumentNullException(nameof(columnIndexMap));

    /// <summary>
    /// Translates a LINQ expression tree to a logical plan.
    /// </summary>
    public LogicalPlanNode Translate(Expression expression)
    {
        // Start with a scan of the source table
        var tableName = _elementType.Name;
        var plan = new ScanPlan(tableName, _source, _schema, rowCount);

        // Walk the expression tree and build up the logical plan
        return TranslateExpression(expression, plan);
    }

    private LogicalPlanNode TranslateExpression(Expression expression, LogicalPlanNode currentPlan)
    {
        // Strip quote if present
        if (expression is UnaryExpression { NodeType: ExpressionType.Quote } unary)
        {
            expression = unary.Operand;
        }

        // Handle method calls (Where, Select, GroupBy, etc.)
        if (expression is MethodCallExpression methodCall)
        {
            // First, process the source expression recursively
            currentPlan = TranslateExpression(methodCall.Arguments[0], currentPlan);

            // Then handle this method
            return methodCall.Method.Name switch
            {
                "Where" => TranslateWhere(methodCall, currentPlan),
                "Select" => TranslateSelect(methodCall, currentPlan),
                "GroupBy" => TranslateGroupBy(methodCall, currentPlan),
                "Take" => TranslateTake(methodCall, currentPlan),
                "Skip" => TranslateSkip(methodCall, currentPlan),
                
                // Aggregates
                "Sum" => TranslateAggregate(methodCall, currentPlan, AggregationOperation.Sum),
                "Average" => TranslateAggregate(methodCall, currentPlan, AggregationOperation.Average),
                "Min" => TranslateAggregate(methodCall, currentPlan, AggregationOperation.Min),
                "Max" => TranslateAggregate(methodCall, currentPlan, AggregationOperation.Max),
                "Count" => TranslateCount(methodCall, currentPlan),
                
                // Terminal operations that don't change the plan structure
                "First" or "FirstOrDefault" or "Single" or "SingleOrDefault" or 
                "Any" or "All" or "ToList" or "ToArray" =>
                    currentPlan, // Plan is unchanged, execution will handle terminal operation
                
                _ => throw new NotSupportedException($"Method '{methodCall.Method.Name}' is not supported in logical plan translation")
            };
        }

        // Base case: constant expression (the initial ArrowQuery object)
        if (expression is ConstantExpression)
        {
            return currentPlan;
        }

        throw new NotSupportedException($"Expression type '{expression.NodeType}' is not supported");
    }

    private FilterPlan TranslateWhere(MethodCallExpression methodCall, LogicalPlanNode input)
    {
        // Extract the predicate lambda
        var lambda = (LambdaExpression)((UnaryExpression)methodCall.Arguments[1]).Operand;
        
        // Create a properly typed lambda for PredicateAnalyzer
        // PredicateAnalyzer expects Expression<Func<T, bool>> where T matches the element type
        var parameter = Expression.Parameter(_elementType, lambda.Parameters[0].Name);
        var rewrittenBody = new ParameterReplacer(lambda.Parameters[0], parameter).Visit(lambda.Body);
        var typedLambda = Expression.Lambda(
            typeof(Func<,>).MakeGenericType(_elementType, typeof(bool)),
            rewrittenBody,
            parameter);
        
        // Analyze the predicate to extract column-level operations
        var analyzeMethod = typeof(PredicateAnalyzer).GetMethod(nameof(PredicateAnalyzer.Analyze))!
            .MakeGenericMethod(_elementType);
        var analysis = (PredicateAnalysisResult)analyzeMethod.Invoke(null, [typedLambda, _columnIndexMap])!;

        if (!analysis.IsFullySupported || analysis.Predicates.Count == 0)
        {
            throw new NotSupportedException(
                $"Predicate not supported for logical plan: {string.Join(", ", analysis.UnsupportedReasons)}");
        }

        // Estimate selectivity based on zone maps or heuristics
        double selectivity = EstimateSelectivity(analysis.Predicates);

        return new FilterPlan(input, analysis.Predicates, selectivity);
    }

    /// <summary>
    /// Expression visitor that replaces parameter references.
    /// </summary>
    private sealed class ParameterReplacer(ParameterExpression oldParameter, ParameterExpression newParameter) : ExpressionVisitor
    {
        protected override Expression VisitParameter(ParameterExpression node)
        {
            return node == oldParameter ? newParameter : base.VisitParameter(node);
        }
    }

    private LogicalPlanNode TranslateSelect(MethodCallExpression methodCall, LogicalPlanNode input)
    {
        // Extract the selector lambda
        var selector = (LambdaExpression)((UnaryExpression)methodCall.Arguments[1]).Operand;
        
        // Special case: GroupBy followed by Select with aggregations
        // Pattern: .GroupBy(x => x.Category).Select(g => new { g.Key, Total = g.Sum(x => x.Sales) })
        if (input is GroupByPlan existingGroupBy 
            && ExpressionHelper.TryExtractAggregations(selector, out var aggregations, out var groupKeyProperty)
            && aggregations is not null)
        {
            // Create a new GroupByPlan with the extracted aggregations and key property name
            var groupByWithAggs = new GroupByPlan(
                existingGroupBy.Input,
                existingGroupBy.GroupByColumn,
                existingGroupBy.GroupByKeyType,
                aggregations,
                groupKeyProperty); // Pass the key property name
            
            return groupByWithAggs;
        }
        
        // Regular Select: Try to extract projections
        if (ExpressionHelper.TryExtractProjections(selector, input.OutputSchema, out var projections) 
            && projections is not null)
        {
            return new ProjectPlan(input, projections);
        }

        // If we can't extract projections, just pass through (project all columns)
        // This happens with complex selectors we don't support yet
        return input;
    }

    private GroupByPlan TranslateGroupBy(MethodCallExpression methodCall, LogicalPlanNode input)
    {
        // Extract key selector
        var keySelector = (LambdaExpression)((UnaryExpression)methodCall.Arguments[1]).Operand;
        
        // Try to extract column name from key selector
        if (!ExpressionHelper.TryExtractColumnName(keySelector, out var groupByColumn) 
            || groupByColumn is null)
        {
            throw new NotSupportedException("GroupBy key selector must be a simple property access (e.g., x => x.Category)");
        }

        // Get the key type from input schema
        if (!input.OutputSchema.TryGetValue(groupByColumn, out var keyType))
        {
            throw new InvalidOperationException($"GroupBy column '{groupByColumn}' not found in input schema");
        }

        // If there's no subsequent Select with aggregations, just return the GroupBy
        // The aggregations will be added when we encounter the Select
        // For now, return a placeholder GroupBy with no aggregations
        return new GroupByPlan(input, groupByColumn, keyType, []);
    }

    private LimitPlan TranslateTake(MethodCallExpression methodCall, LogicalPlanNode input)
    {
        // Extract count
        var countExpr = methodCall.Arguments[1];
        if (countExpr is ConstantExpression { Value: int count })
        {
            return new LimitPlan(input, count);
        }

        throw new NotSupportedException("Take count must be a constant integer");
    }

    private OffsetPlan TranslateSkip(MethodCallExpression methodCall, LogicalPlanNode input)
    {
        // Extract count
        var countExpr = methodCall.Arguments[1];
        if (countExpr is ConstantExpression { Value: int count })
        {
            return new OffsetPlan(input, count);
        }

        throw new NotSupportedException("Skip count must be a constant integer");
    }

    private AggregatePlan TranslateAggregate(
        MethodCallExpression methodCall, 
        LogicalPlanNode input, 
        AggregationOperation operation)
    {
        // Check if there's a selector lambda (e.g., Sum(x => x.Price))
        string? columnName = null;
        Type outputType = operation switch
        {
            AggregationOperation.Count => typeof(long),
            AggregationOperation.Sum => typeof(long), // Simplified - would need actual column type
            AggregationOperation.Average => typeof(double),
            AggregationOperation.Min => typeof(object), // Would need actual column type
            AggregationOperation.Max => typeof(object), // Would need actual column type
            _ => typeof(object)
        };

        if (methodCall.Arguments.Count > 1)
        {
            var selector = (LambdaExpression)((UnaryExpression)methodCall.Arguments[1]).Operand;
            
            // Try to extract column name
            if (ExpressionHelper.TryExtractColumnName(selector, out var extractedColumn))
            {
                columnName = extractedColumn;
                
                // Try to get more accurate output type from input schema
                if (columnName is not null && input.OutputSchema.TryGetValue(columnName, out var columnType))
                {
                    outputType = operation switch
                    {
                        AggregationOperation.Average => typeof(double),
                        AggregationOperation.Count => typeof(long),
                        _ => columnType
                    };
                }
            }
        }

        return new AggregatePlan(input, operation, columnName, outputType);
    }

    private AggregatePlan TranslateCount(MethodCallExpression methodCall, LogicalPlanNode input)
    {
        return new AggregatePlan(input, AggregationOperation.Count, null, typeof(long));
    }

    private double EstimateSelectivity(IReadOnlyList<ColumnPredicate> predicates)
    {
        // Simple heuristic for now
        // With zone maps, we can get much better estimates
        return Math.Pow(0.5, predicates.Count); // Each predicate cuts by ~50%
    }

    private LambdaExpression ConvertLambda(LambdaExpression lambda, Type targetType)
    {
        // Convert lambda to expected type for PredicateAnalyzer
        // This is a simplified version - full implementation would handle type conversion
        return lambda;
    }
}
