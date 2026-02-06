using System.Linq.Expressions;

namespace FrozenArrow.Query.LogicalPlan;

/// <summary>
/// Translates LINQ Expression trees to logical plans.
/// This is a thin adapter layer that decouples LINQ from the query engine.
/// </summary>
public sealed class LinqToLogicalPlanTranslator
{
    private readonly object _source;
    private readonly Type _elementType;
    private readonly IReadOnlyDictionary<string, Type> _schema;
    private readonly Dictionary<string, int> _columnIndexMap;
    private readonly long _rowCount;

    public LinqToLogicalPlanTranslator(
        object source,
        Type elementType,
        IReadOnlyDictionary<string, Type> schema,
        Dictionary<string, int> columnIndexMap,
        long rowCount)
    {
        _source = source ?? throw new ArgumentNullException(nameof(source));
        _elementType = elementType ?? throw new ArgumentNullException(nameof(elementType));
        _schema = schema ?? throw new ArgumentNullException(nameof(schema));
        _columnIndexMap = columnIndexMap ?? throw new ArgumentNullException(nameof(columnIndexMap));
        _rowCount = rowCount;
    }

    /// <summary>
    /// Translates a LINQ expression tree to a logical plan.
    /// </summary>
    public LogicalPlanNode Translate(Expression expression)
    {
        // Start with a scan of the source table
        var tableName = _elementType.Name;
        var plan = new ScanPlan(tableName, _source, _schema, _rowCount);

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

    private LogicalPlanNode TranslateWhere(MethodCallExpression methodCall, LogicalPlanNode input)
    {
        // Extract the predicate lambda
        var lambda = (LambdaExpression)((UnaryExpression)methodCall.Arguments[1]).Operand;
        
        // Analyze the predicate to extract column-level operations
        var analysis = PredicateAnalyzer.Analyze(
            (Expression<Func<object, bool>>)ConvertLambda(lambda, typeof(object)),
            _columnIndexMap);

        if (!analysis.IsFullySupported || analysis.Predicates.Count == 0)
        {
            throw new NotSupportedException(
                $"Predicate not supported for logical plan: {string.Join(", ", analysis.UnsupportedReasons)}");
        }

        // Estimate selectivity based on zone maps or heuristics
        double selectivity = EstimateSelectivity(analysis.Predicates);

        return new FilterPlan(input, analysis.Predicates, selectivity);
    }

    private LogicalPlanNode TranslateSelect(MethodCallExpression methodCall, LogicalPlanNode input)
    {
        // For now, we only support simple column projections
        // Full expression support would require more analysis
        
        // TODO: Analyze selector lambda to extract projections
        // For MVP, just pass through (project all columns)
        return input;
    }

    private LogicalPlanNode TranslateGroupBy(MethodCallExpression methodCall, LogicalPlanNode input)
    {
        // Extract key selector
        var keySelector = (LambdaExpression)((UnaryExpression)methodCall.Arguments[1]).Operand;
        
        // TODO: Analyze key selector to extract column name
        // For MVP, just pass through
        return input;
    }

    private LogicalPlanNode TranslateTake(MethodCallExpression methodCall, LogicalPlanNode input)
    {
        // Extract count
        var countExpr = methodCall.Arguments[1];
        if (countExpr is ConstantExpression { Value: int count })
        {
            return new LimitPlan(input, count);
        }

        throw new NotSupportedException("Take count must be a constant integer");
    }

    private LogicalPlanNode TranslateSkip(MethodCallExpression methodCall, LogicalPlanNode input)
    {
        // Extract count
        var countExpr = methodCall.Arguments[1];
        if (countExpr is ConstantExpression { Value: int count })
        {
            return new OffsetPlan(input, count);
        }

        throw new NotSupportedException("Skip count must be a constant integer");
    }

    private LogicalPlanNode TranslateAggregate(
        MethodCallExpression methodCall, 
        LogicalPlanNode input, 
        AggregationOperation operation)
    {
        // Check if there's a selector lambda (e.g., Sum(x => x.Price))
        string? columnName = null;
        Type outputType = typeof(long); // Default

        if (methodCall.Arguments.Count > 1)
        {
            var selector = (LambdaExpression)((UnaryExpression)methodCall.Arguments[1]).Operand;
            // TODO: Extract column name from selector
            // For MVP, throw if selector is present
            throw new NotSupportedException("Aggregate with selector not yet supported in logical plan");
        }

        return new AggregatePlan(input, operation, columnName, outputType);
    }

    private LogicalPlanNode TranslateCount(MethodCallExpression methodCall, LogicalPlanNode input)
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
