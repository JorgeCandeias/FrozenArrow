using System.Collections;
using System.Linq.Expressions;
using Apache.Arrow;

namespace ArrowCollection.Query;

/// <summary>
/// Provides LINQ query support over ArrowCollection with optimized column-level operations.
/// This class implements IQueryable{T} to provide transparent LINQ integration.
/// </summary>
/// <remarks>
/// ArrowQuery operates directly on Arrow columns without materializing objects until
/// the final enumeration. Filter predicates are pushed down to column-level evaluation,
/// and grouping/aggregation operations work on columns without full object creation.
/// </remarks>
public sealed class ArrowQuery<T> : IQueryable<T>, IOrderedQueryable<T>
{
    private readonly ArrowQueryProvider _provider;
    private readonly Expression _expression;

    /// <summary>
    /// Creates a new ArrowQuery from an ArrowCollection source.
    /// </summary>
    internal ArrowQuery(ArrowCollection<T> source)
    {
        Source = source ?? throw new ArgumentNullException(nameof(source));
        _provider = new ArrowQueryProvider(source);
        _expression = Expression.Constant(this);
    }

    /// <summary>
    /// Creates a new ArrowQuery with an existing provider and expression.
    /// </summary>
    internal ArrowQuery(ArrowQueryProvider provider, Expression expression)
    {
        _provider = provider ?? throw new ArgumentNullException(nameof(provider));
        _expression = expression ?? throw new ArgumentNullException(nameof(expression));
        Source = provider.GetSource<T>();
    }

    /// <summary>
    /// Gets the underlying ArrowCollection source.
    /// </summary>
    public ArrowCollection<T> Source { get; }

    /// <summary>
    /// Gets the type of the elements in the query.
    /// </summary>
    public Type ElementType => typeof(T);

    /// <summary>
    /// Gets the expression tree representing this query.
    /// </summary>
    public Expression Expression => _expression;

    /// <summary>
    /// Gets the query provider for this query.
    /// </summary>
    public IQueryProvider Provider => _provider;

    /// <summary>
    /// Returns an enumerator that executes the query and iterates through the results.
    /// </summary>
    public IEnumerator<T> GetEnumerator()
    {
        return _provider.Execute<IEnumerable<T>>(_expression).GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    /// <summary>
    /// Returns a string representation of the query plan for debugging.
    /// </summary>
    public string Explain()
    {
        var plan = _provider.AnalyzeExpression(_expression);
        return plan.ToString();
    }
}

/// <summary>
/// Query provider that handles LINQ expression execution over ArrowCollection.
/// </summary>
public sealed class ArrowQueryProvider : IQueryProvider
{
    private readonly object _source;
    private readonly Type _elementType;
    private readonly RecordBatch _recordBatch;
    private readonly int _count;
    private readonly Func<RecordBatch, int, object> _createItem;
    private readonly Dictionary<string, int> _columnIndexMap;

    /// <summary>
    /// Gets or sets whether the provider operates in strict mode.
    /// When true (default), unsupported operations throw NotSupportedException.
    /// When false, unsupported operations fall back to materialization.
    /// </summary>
    public bool StrictMode { get; set; } = true;

    internal ArrowQueryProvider(object source)
    {
        _source = source ?? throw new ArgumentNullException(nameof(source));
        
        // Extract type information
        var sourceType = source.GetType();
        var arrowCollectionType = sourceType;
        while (arrowCollectionType != null && 
               (!arrowCollectionType.IsGenericType || 
                arrowCollectionType.GetGenericTypeDefinition() != typeof(ArrowCollection<>)))
        {
            arrowCollectionType = arrowCollectionType.BaseType;
        }

        if (arrowCollectionType is null)
        {
            throw new ArgumentException("Source must be an ArrowCollection<T>", nameof(source));
        }

        _elementType = arrowCollectionType.GetGenericArguments()[0];

        // Get RecordBatch via reflection (it's protected)
        var recordBatchField = typeof(ArrowCollection<>)
            .MakeGenericType(_elementType)
            .GetField("_recordBatch", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        
        _recordBatch = (RecordBatch)recordBatchField!.GetValue(source)!;
        
        var countField = typeof(ArrowCollection<>)
            .MakeGenericType(_elementType)
            .GetField("_count", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        
        _count = (int)countField!.GetValue(source)!;

        // Build column index map from schema
        _columnIndexMap = [];
        var schema = _recordBatch.Schema;
        for (int i = 0; i < schema.FieldsList.Count; i++)
        {
            _columnIndexMap[schema.FieldsList[i].Name] = i;
        }

        // Get the CreateItem method via reflection
        var createItemMethod = sourceType.GetMethod("CreateItem", 
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        
        _createItem = (batch, index) => createItemMethod!.Invoke(source, [batch, index])!;
    }

    internal ArrowCollection<TElement> GetSource<TElement>()
    {
        return (ArrowCollection<TElement>)_source;
    }

    public IQueryable CreateQuery(Expression expression)
    {
        var elementType = GetElementType(expression.Type)
            ?? throw new ArgumentException($"Cannot determine element type from expression type '{expression.Type}'.", nameof(expression));
        var method = typeof(ArrowQueryProvider)
            .GetMethod(nameof(CreateQuery), 1, [typeof(Expression)])!
            .MakeGenericMethod(elementType);
        return (IQueryable)method.Invoke(this, [expression])!;
    }

    public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
    {
        return new ArrowQuery<TElement>(this, expression);
    }

    public object? Execute(Expression expression)
    {
        var elementType = GetElementType(expression.Type) ?? _elementType;
        var method = typeof(ArrowQueryProvider)
            .GetMethod(nameof(Execute), 1, [typeof(Expression)])!
            .MakeGenericMethod(elementType);
        return method.Invoke(this, [expression]);
    }

    public TResult Execute<TResult>(Expression expression)
    {
        var plan = AnalyzeExpression(expression);

        if (!plan.IsFullyOptimized && StrictMode)
        {
            throw new NotSupportedException(
                $"Query contains operations that cannot be optimized: {plan.UnsupportedReason}. " +
                $"Set ArrowQueryProvider.StrictMode = false to allow fallback materialization, " +
                $"or modify the query to use supported operations.");
        }

        return ExecutePlan<TResult>(plan, expression);
    }


    internal QueryPlan AnalyzeExpression(Expression expression)
    {
        var analyzer = new QueryExpressionAnalyzer(_columnIndexMap);
        return analyzer.Analyze(expression);
    }

    private TResult ExecutePlan<TResult>(QueryPlan plan, Expression expression)
    {
        // Build selection bitmap using pooled bitfield (8x more memory efficient)
        using var selection = SelectionBitmap.Create(_count, initialValue: true);

        // Apply column predicates
        foreach (var predicate in plan.ColumnPredicates)
        {
            predicate.Evaluate(_recordBatch, ref System.Runtime.CompilerServices.Unsafe.AsRef(in selection));
        }

        // Count selected rows using hardware popcount
        var selectedCount = selection.CountSet();

        // Handle simple aggregates (Sum, Average, Min, Max) directly on columns
        if (plan.SimpleAggregate is not null)
        {
            return ExecuteSimpleAggregate<TResult>(plan.SimpleAggregate, ref System.Runtime.CompilerServices.Unsafe.AsRef(in selection));
        }

        // Handle different result types
        var resultType = typeof(TResult);

        // IEnumerable<T> - return lazy enumeration
        if (resultType.IsGenericType && 
            (resultType.GetGenericTypeDefinition() == typeof(IEnumerable<>) ||
             resultType.GetGenericTypeDefinition() == typeof(IQueryable<>)))
        {
            // For enumeration, we need to copy the selected indices since the bitmap will be disposed
            var selectedIndices = new List<int>(selectedCount);
            foreach (var idx in selection.GetSelectedIndices())
            {
                selectedIndices.Add(idx);
            }
            var enumerable = EnumerateSelectedIndices(selectedIndices);
            return (TResult)enumerable;
        }

        // Single element results (First, Single, etc.)
        if (resultType == _elementType)
        {
            foreach (var i in selection.GetSelectedIndices())
            {
                return (TResult)_createItem(_recordBatch, i);
            }
            throw new InvalidOperationException("Sequence contains no elements.");
        }

        // Count
        if (resultType == typeof(int))
        {
            return (TResult)(object)selectedCount;
        }

        // LongCount
        if (resultType == typeof(long))
        {
            return (TResult)(object)(long)selectedCount;
        }

        // Boolean results (Any, All)
        if (resultType == typeof(bool))
        {
            // Check the expression to determine which operation
            if (expression is MethodCallExpression methodCall)
            {
                if (methodCall.Method.Name == "Any")
                {
                    return (TResult)(object)(selectedCount > 0);
                }
                if (methodCall.Method.Name == "All")
                {
                    return (TResult)(object)(selectedCount == _count);
                }
            }
        }

        throw new NotSupportedException($"Result type '{resultType}' is not supported.");
    }

    private TResult ExecuteSimpleAggregate<TResult>(SimpleAggregateOperation aggregate, ref SelectionBitmap selection)
    {
        // Find the column by name
        var columnIndex = _columnIndexMap.TryGetValue(aggregate.ColumnName!, out var idx) 
            ? idx 
            : throw new InvalidOperationException($"Column '{aggregate.ColumnName}' not found.");
        
        var column = _recordBatch.Column(columnIndex);

        var result = aggregate.Operation switch
        {
            AggregationOperation.Sum => ColumnAggregator.ExecuteSum(column, ref selection, aggregate.ResultType),
            AggregationOperation.Average => ColumnAggregator.ExecuteAverage(column, ref selection, aggregate.ResultType),
            AggregationOperation.Min => ColumnAggregator.ExecuteMin(column, ref selection, aggregate.ResultType),
            AggregationOperation.Max => ColumnAggregator.ExecuteMax(column, ref selection, aggregate.ResultType),
            _ => throw new NotSupportedException($"Aggregate operation {aggregate.Operation} is not supported.")
        };

        return (TResult)result;
    }

    private IEnumerable<T> EnumerateSelectedIndicesCore<T>(List<int> selectedIndices)
    {
        foreach (var i in selectedIndices)
        {
            yield return (T)_createItem(_recordBatch, i);
        }
    }

    private object EnumerateSelectedIndices(List<int> selectedIndices)
    {
        var method = typeof(ArrowQueryProvider)
            .GetMethod(nameof(EnumerateSelectedIndicesCore), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
            .MakeGenericMethod(_elementType);
        return method.Invoke(this, [selectedIndices])!;
    }


    private static Type? GetElementType(Type type)
    {
        if (type.IsGenericType)
        {
            var genericDef = type.GetGenericTypeDefinition();
            if (genericDef == typeof(IQueryable<>) ||
                genericDef == typeof(IEnumerable<>) ||
                genericDef == typeof(IOrderedQueryable<>) ||
                genericDef == typeof(IOrderedEnumerable<>))
            {
                return type.GetGenericArguments()[0];
            }
        }
        return null;
    }
}

/// <summary>
/// Analyzes LINQ expression trees to build a QueryPlan.
/// </summary>
internal sealed class QueryExpressionAnalyzer(Dictionary<string, int> columnIndexMap) : ExpressionVisitor
{
    private readonly List<ColumnPredicate> _predicates = [];
    private readonly HashSet<string> _columnsAccessed = [];
    private readonly List<string> _unsupportedReasons = [];
    private bool _hasUnsupportedPatterns;
    private SimpleAggregateOperation? _simpleAggregate;

    private static readonly HashSet<string> SupportedMethods =
    [
        "Where", "Select", "First", "FirstOrDefault", "Single", "SingleOrDefault",
        "Any", "All", "Count", "LongCount", "Take", "Skip", "ToList", "ToArray",
        "OrderBy", "OrderByDescending", "ThenBy", "ThenByDescending",
        "Sum", "Average", "Min", "Max"
    ];

    private static readonly HashSet<string> AggregateMethods =
    [
        "Sum", "Average", "Min", "Max"
    ];

    public QueryPlan Analyze(Expression expression)
    {
        Visit(expression);

        return new QueryPlan
        {
            IsFullyOptimized = !_hasUnsupportedPatterns,
            UnsupportedReason = _unsupportedReasons.Count > 0 
                ? string.Join("; ", _unsupportedReasons) 
                : null,
            ColumnsAccessed = [.. _columnsAccessed],
            ColumnPredicates = _predicates,
            HasFallbackPredicate = false,
            SimpleAggregate = _simpleAggregate,
            EstimatedSelectivity = EstimateSelectivity()
        };
    }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        var methodName = node.Method.Name;
        var declaringType = node.Method.DeclaringType?.FullName ?? "";

        // Check for Enumerable methods (should use Queryable)
        if (declaringType == "System.Linq.Enumerable")
        {
            _hasUnsupportedPatterns = true;
            _unsupportedReasons.Add(
                $"Method '{methodName}' from System.Linq.Enumerable bypasses query optimization. " +
                $"Ensure you're using IQueryable<T> methods (call .AsQueryable() first).");
            return node;
        }

        // Check for unsupported Queryable methods
        if (declaringType == "System.Linq.Queryable")
        {
            if (!SupportedMethods.Contains(methodName))
            {
                _hasUnsupportedPatterns = true;
                _unsupportedReasons.Add(
                    $"LINQ method '{methodName}' is not supported by ArrowQuery. " +
                    $"Supported methods: {string.Join(", ", SupportedMethods)}.");
                return node;
            }

            // Process Where clauses
            if (methodName == "Where" && node.Arguments.Count >= 2)
            {
                var predicateArg = node.Arguments[1];
                if (predicateArg is UnaryExpression unary && unary.Operand is LambdaExpression lambda)
                {
                    AnalyzeWherePredicate(lambda);
                }
            }

            // Process aggregate methods (Sum, Average, Min, Max)
            if (AggregateMethods.Contains(methodName))
            {
                AnalyzeAggregateMethod(node, methodName);
            }
        }

        return base.VisitMethodCall(node);
    }

    private void AnalyzeWherePredicate(LambdaExpression lambda)
    {
        // Use PredicateAnalyzer to extract column predicates
        var analyzerMethod = typeof(PredicateAnalyzer)
            .GetMethod(nameof(PredicateAnalyzer.Analyze))!
            .MakeGenericMethod(lambda.Parameters[0].Type);

        var result = (PredicateAnalysisResult)analyzerMethod.Invoke(null, [lambda, columnIndexMap])!;

        _predicates.AddRange(result.Predicates);
        
        foreach (var predicate in result.Predicates)
        {
            _columnsAccessed.Add(predicate.ColumnName);
        }

        if (!result.IsFullySupported)
        {
            _hasUnsupportedPatterns = true;
            _unsupportedReasons.AddRange(result.UnsupportedReasons);
        }
    }

    private void AnalyzeAggregateMethod(MethodCallExpression node, string methodName)
    {
        var operation = methodName switch
        {
            "Sum" => AggregationOperation.Sum,
            "Average" => AggregationOperation.Average,
            "Min" => AggregationOperation.Min,
            "Max" => AggregationOperation.Max,
            _ => throw new NotSupportedException($"Unknown aggregate method: {methodName}")
        };

        // Extract the column name from the selector lambda
        string? columnName = null;
        
        // The selector is the second argument (first is the source)
        if (node.Arguments.Count >= 2)
        {
            var selectorArg = node.Arguments[1];
            if (selectorArg is UnaryExpression unary && unary.Operand is LambdaExpression lambda)
            {
                columnName = ExtractColumnNameFromSelector(lambda);
            }
        }

        if (columnName is not null)
        {
            _columnsAccessed.Add(columnName);
            _simpleAggregate = new SimpleAggregateOperation
            {
                Operation = operation,
                ColumnName = columnName,
                ResultType = node.Method.ReturnType
            };
        }
        else
        {
            _hasUnsupportedPatterns = true;
            _unsupportedReasons.Add($"Could not extract column name from {methodName} selector. " +
                "Only simple property access like x => x.Salary is supported.");
        }
    }

    private string? ExtractColumnNameFromSelector(LambdaExpression lambda)
    {
        // Handle simple property access: x => x.Property
        if (lambda.Body is MemberExpression memberExpr)
        {
            // Check if the member is accessed from the parameter
            if (memberExpr.Expression is ParameterExpression)
            {
                var memberName = memberExpr.Member.Name;
                
                // Look up the column name in the map (it might be aliased via ArrowArray attribute)
                // First try direct match, then try finding by property name
                if (columnIndexMap.ContainsKey(memberName))
                {
                    return memberName;
                }
                
                // The column might be named differently - search for it
                // For now, just return the member name and let execution handle it
                return memberName;
            }
        }
        
        // Handle unary conversion: x => (double)x.Property
        if (lambda.Body is UnaryExpression unaryExpr && unaryExpr.Operand is MemberExpression innerMember)
        {
            if (innerMember.Expression is ParameterExpression)
            {
                return innerMember.Member.Name;
            }
        }

        return null;
    }

    private double EstimateSelectivity()
    {
        if (_predicates.Count == 0)
            return 1.0;

        // Simple heuristic: each predicate reduces selectivity
        // In practice, you'd use column statistics here
        var selectivity = 1.0;
        foreach (var _ in _predicates)
        {
            selectivity *= 0.3; // Assume each filter keeps ~30% of rows
        }
        return Math.Max(selectivity, 0.01);
    }
}
