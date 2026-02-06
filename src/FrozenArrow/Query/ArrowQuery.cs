using System.Collections;
using System.Linq.Expressions;
using Apache.Arrow;

namespace FrozenArrow.Query;

/// <summary>
/// Provides LINQ query support over FrozenArrow with optimized column-level operations.
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
    /// Creates a new ArrowQuery from a FrozenArrow source.
    /// </summary>
    internal ArrowQuery(FrozenArrow<T> source)
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
        // Source is only valid for the original element type, not intermediate types like IGrouping
        Source = null!;
    }

    /// <summary>
    /// Gets the underlying FrozenArrow source (only valid for the original element type).
    /// </summary>
    public FrozenArrow<T>? Source { get; }

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
/// Query provider that handles LINQ expression execution over FrozenArrow.
/// </summary>
public sealed partial class ArrowQueryProvider : IQueryProvider
{
    private readonly object _source;
    private readonly Type _elementType;
    private readonly RecordBatch _recordBatch;
    private readonly int _count;
    private readonly Func<RecordBatch, int, object> _createItem;
    private readonly Dictionary<string, int> _columnIndexMap;
    private readonly ZoneMap? _zoneMap;
    private readonly QueryPlanCache _queryPlanCache;

    /// <summary>
    /// Gets or sets whether the provider operates in strict mode.
    /// When true (default), unsupported operations throw NotSupportedException.
    /// When false, unsupported operations fall back to materialization.
    /// </summary>
    public bool StrictMode { get; set; } = true;

    /// <summary>
    /// Gets or sets the options for parallel query execution.
    /// Set to null to use default options, or customize for specific workloads.
    /// </summary>
    public ParallelQueryOptions? ParallelOptions { get; set; }

    /// <summary>
    /// Gets or sets the options for query plan caching.
    /// Set to null to use default options, or customize for specific workloads.
    /// </summary>
    public QueryPlanCacheOptions? QueryPlanCacheOptions { get; set; }

    /// <summary>
    /// Gets the query plan cache statistics for monitoring cache performance.
    /// </summary>
    public CacheStatistics QueryPlanCacheStatistics => _queryPlanCache.Statistics;

    /// <summary>
    /// Gets the number of cached query plans.
    /// </summary>
    public int CachedQueryPlanCount => _queryPlanCache.Count;

    /// <summary>
    /// Clears the query plan cache. Useful when memory pressure is high
    /// or when testing cache behavior.
    /// </summary>
    public void ClearQueryPlanCache() => _queryPlanCache.Clear();

    internal ArrowQueryProvider(object source)
    {
        _source = source ?? throw new ArgumentNullException(nameof(source));
        
        // Extract type information
        var sourceType = source.GetType();
        var arrowCollectionType = sourceType;
        while (arrowCollectionType != null && 
               (!arrowCollectionType.IsGenericType || 
                arrowCollectionType.GetGenericTypeDefinition() != typeof(FrozenArrow<>)))
        {
            arrowCollectionType = arrowCollectionType.BaseType;
        }

        if (arrowCollectionType is null)
        {
            throw new ArgumentException("Source must be an FrozenArrow<T>", nameof(source));
        }

        _elementType = arrowCollectionType.GetGenericArguments()[0];

        // Use cached delegate to extract values - eliminates MakeGenericMethod + Invoke overhead
        // First call for a type pays one-time reflection cost to create delegate,
        // subsequent calls for same type use cached delegate (fast path)
        var (recordBatch, count, columnIndexMap, createItem, zoneMap, queryPlanCache) = 
            TypedQueryProviderCache.ExtractSourceData(_elementType, source);
        
        _recordBatch = recordBatch;
        _count = count;
        _columnIndexMap = columnIndexMap;
        _createItem = createItem;
        _zoneMap = zoneMap;
        _queryPlanCache = queryPlanCache;
    }

    internal FrozenArrow<TElement> GetSource<TElement>()
    {
        return (FrozenArrow<TElement>)_source;
    }

    public IQueryable CreateQuery(Expression expression)
    {
        var elementType = GetElementType(expression.Type)
            ?? throw new ArgumentException($"Cannot determine element type from expression type '{expression.Type}'.", nameof(expression));
        
        // Use cached delegate to eliminate MakeGenericMethod + Invoke overhead
        return TypedQueryProviderCache.CreateQuery(this, elementType, expression);
    }

    public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
    {
        return new ArrowQuery<TElement>(this, expression);
    }

    public object? Execute(Expression expression)
    {
        var elementType = GetElementType(expression.Type) ?? _elementType;
        
        // Use cached delegate to eliminate MakeGenericMethod + Invoke overhead
        return TypedQueryProviderCache.Execute(this, elementType, expression);
    }

    /// <summary>
    /// Gets or sets whether to use the new logical plan execution path.
    /// When true, queries will be translated to logical plans, optimized, and executed.
    /// When false (default), queries use the existing QueryPlan path.
    /// This is a feature flag to enable gradual rollout of the new architecture.
    /// </summary>
    public bool UseLogicalPlanExecution { get; set; } = false;

    /// <summary>
    /// Gets or sets whether to use direct logical plan execution (Phase 5).
    /// When true, logical plans are executed directly without converting to QueryPlan.
    /// When false, uses the bridge pattern (convert to QueryPlan then execute).
    /// Only applies when UseLogicalPlanExecution is true.
    /// </summary>
    public bool UseDirectLogicalPlanExecution { get; set; } = false;

    /// <summary>
    /// Gets or sets whether to use physical plan execution (Phase 6).
    /// When true, logical plans are converted to physical plans with strategy selection,
    /// then executed with the physical executor.
    /// Only applies when UseLogicalPlanExecution is true.
    /// </summary>
    public bool UsePhysicalPlanExecution { get; set; } = false;

    public TResult Execute<TResult>(Expression expression)
    {
        // NEW PATH: Try logical plan execution if enabled
        if (UseLogicalPlanExecution)
        {
            try
            {
                return ExecuteWithLogicalPlan<TResult>(expression);
            }
            catch (NotSupportedException)
            {
                // Fall back to old path if logical plan translation fails
                // This allows gradual adoption of the new architecture
            }
        }

        // EXISTING PATH: Use current QueryPlan-based execution
        var plan = AnalyzeExpression(expression);

        if (!plan.IsFullyOptimized && StrictMode)
        {
            throw new NotSupportedException(
                $"Query contains operations that cannot be optimized: {plan.UnsupportedReason}. " +
                $"Set ArrowQueryProvider.StrictMode = false to allow fallback materialization, " +
                $"or modify the query to use supported operations.");
        }

        // FALLBACK PATH: If query is not fully optimized and strict mode is disabled,
        // fall back to LINQ-to-Objects by materializing the entire collection
        if (!plan.IsFullyOptimized && !StrictMode)
        {
            return ExecuteWithFallback<TResult>(expression);
        }

        return ExecutePlan<TResult>(plan, expression);
    }


    internal QueryPlan AnalyzeExpression(Expression expression)
    {
        // Check cache first - this can eliminate ~2-3ms of expression analysis
        if (_queryPlanCache.TryGetPlan(expression, out var cachedPlan))
        {
            return cachedPlan!;
        }

        // Cache miss - analyze the expression
        var analyzer = new QueryExpressionAnalyzer(_columnIndexMap);
        var plan = analyzer.Analyze(expression);

        // Cache the result for future queries with the same structure
        _queryPlanCache.CachePlan(expression, plan);

        return plan;
    }

    /// <summary>
    /// Executes a query by falling back to LINQ-to-Objects.
    /// This is used when the query contains operations that cannot be optimized
    /// and StrictMode is disabled.
    /// </summary>
    /// <remarks>
    /// WARNING: This materializes the entire collection into memory, which can be
    /// expensive for large datasets. This is only used as a fallback for unsupported
    /// operations like modulo, OR expressions, external method calls, etc.
    /// </remarks>
    private TResult ExecuteWithFallback<TResult>(Expression expression)
    {
        // Materialize all records from the RecordBatch into a List
        var list = MaterializeAllRecords();

        // Create a queryable from the materialized list
        var queryable = list.AsQueryable();

        // Rewrite the expression tree to replace references to the ArrowQuery
        // with references to the materialized queryable
        var rewriter = new FallbackExpressionRewriter(queryable);
        var rewrittenExpression = rewriter.Visit(expression);

        // Execute the query using standard LINQ-to-Objects
        return queryable.Provider.Execute<TResult>(rewrittenExpression);
    }

    /// <summary>
    /// Finds the source constant expression in the expression tree.
    /// </summary>
    private static ConstantExpression? FindSourceConstant(Expression expression)
    {
        if (expression is ConstantExpression constant)
        {
            return constant;
        }

        if (expression is MethodCallExpression methodCall)
        {
            // The source is typically the first argument to LINQ methods
            if (methodCall.Arguments.Count > 0)
            {
                return FindSourceConstant(methodCall.Arguments[0]);
            }
        }

        return null;
    }

    /// <summary>
    /// Materializes all records from the RecordBatch into a List.
    /// </summary>
    private IList MaterializeAllRecords()
    {
        // Use reflection to create a List<T> of the correct element type
        var listType = typeof(List<>).MakeGenericType(_elementType);
        var list = (IList)Activator.CreateInstance(listType, _count)!;

        // Enumerate all records and add them to the list
        for (int i = 0; i < _count; i++)
        {
            var item = _createItem(_recordBatch, i);
            list.Add(item);
        }

        return list;
    }

    /// <summary>
    /// Expression visitor that rewrites expressions to work with a fallback queryable.
    /// This replaces references to ArrowQuery with the materialized queryable.
    /// </summary>
    private sealed class FallbackExpressionRewriter(IQueryable fallbackQueryable) : ExpressionVisitor
    {
        private ConstantExpression? _sourceConstant;

        protected override Expression VisitConstant(ConstantExpression node)
        {
            // If this is the first constant we find and it's a queryable,
            // assume it's the source and replace it
            if (_sourceConstant == null && node.Type.IsGenericType)
            {
                var genericType = node.Type.GetGenericTypeDefinition();
                if (genericType == typeof(ArrowQuery<>))
                {
                    _sourceConstant = node;
                    return Expression.Constant(fallbackQueryable, fallbackQueryable.GetType());
                }
            }

            // If we've already found the source, replace any references to it
            if (_sourceConstant != null && node == _sourceConstant)
            {
                return Expression.Constant(fallbackQueryable, fallbackQueryable.GetType());
            }

            return base.VisitConstant(node);
        }
    }






    private TResult ExecutePlan<TResult>(QueryPlan plan, Expression expression)
    {
        // Detect short-circuit operations that can benefit from streaming evaluation
        var resultType = typeof(TResult);
        var isShortCircuit = false;
        string? shortCircuitOp = null;

        if (expression is MethodCallExpression methodCall)
        {
            shortCircuitOp = methodCall.Method.Name;
            isShortCircuit = shortCircuitOp is "Any" or "First" or "FirstOrDefault" or "Single" or "SingleOrDefault";
        }

        // STREAMING PATH: For short-circuit operations, avoid building full bitmap
        // This can be orders of magnitude faster when matches are found early
        // However, if Skip is present, we can't short-circuit since we need to skip elements
        if (isShortCircuit && plan.ColumnPredicates.Count > 0 && !plan.Skip.HasValue)
        {
            return ExecuteShortCircuit<TResult>(plan, shortCircuitOp!);
        }

        // Try fused execution for filtered aggregates (single-pass optimization)
        if (FusedAggregator.CanUseFusedExecution(plan, _count, _recordBatch, _columnIndexMap))
        {
            return ExecuteFusedAggregate<TResult>(plan);
        }

        // PAGINATION OPTIMIZATION: For .Where(...).Skip(N).Take(M) with ToList/materialization,
        // collect indices with early termination instead of building full bitmap
        // This is beneficial even for dense queries when Take is small relative to total matches
        // Example: .Where(x => x.IsActive).Skip(1000).Take(100).ToList()
        //   - Without optimization: evaluate all ~700K matches, build 125KB bitmap, then skip/take
        //   - With optimization: stop after finding 1100 matches (10-50x faster for pagination)
        //
        // IMPORTANT: This optimization is ONLY safe when there's no OrderBy/GroupBy/etc. between
        // the Where and Take. Early termination assumes we can stop collecting after N matches,
        // but OrderBy changes which rows are in the "first N" after sorting.
        //
        // We detect this by checking IsFullyOptimized - if the query has unsupported operations
        // like OrderBy, it won't be fully optimized and we skip this optimization.
        bool isPaginatedEnumeration = plan.IsFullyOptimized &&  // ? Must be fully optimized (no OrderBy, etc.)
                                      !plan.PaginationBeforePredicates && 
                                      plan.Take.HasValue && 
                                      plan.ColumnPredicates.Count > 0 &&
                                      (resultType.IsGenericType && 
                                       (resultType.GetGenericTypeDefinition() == typeof(IEnumerable<>) ||
                                        resultType.GetGenericTypeDefinition() == typeof(IQueryable<>)));
        
        if (isPaginatedEnumeration)
        {
            int skipCount = plan.Skip ?? 0;
            int takeCount = plan.Take!.Value;
            int maxIndicesToCollect = skipCount + takeCount;
            
            // Use sparse collection with early termination
            var matchingIndices = SparseIndexCollector.CollectMatchingIndices(
                _recordBatch,
                plan.ColumnPredicates,
                _zoneMap,
                ParallelOptions,
                null,  // No row range limit
                0,     // Start from beginning
                maxIndicesToCollect);
            
            return ExecuteWithSparseIndices<TResult>(plan, matchingIndices, resultType);
        }

        // SPARSE PATH: For highly selective queries (<5%), collect indices directly
        // This avoids materializing a 125KB bitmap when only 1-5% of rows match
        // Memory savings: 1% selectivity ? 40KB list vs 125KB bitmap (3× savings)
        // 
        // Skip sparse path for Count/LongCount - bitmap PopCount is faster than collecting indices
        var isCountQuery = resultType == typeof(int) || resultType == typeof(long);
        if (plan.EstimatedSelectivity < 0.05 && plan.ColumnPredicates.Count > 0 && !isCountQuery)
        {
            // Calculate effective row range for pagination-before-predicates patterns
            int sparseStartRow = 0;
            int? sparseEndRow = null;
            int? maxIndicesToCollect = null;
            
            if (plan.PaginationBeforePredicates && plan.ColumnPredicates.Count > 0)
            {
                sparseStartRow = plan.Skip ?? 0;
                sparseEndRow = _count;
                if (plan.Take.HasValue)
                {
                    sparseEndRow = Math.Min(_count, sparseStartRow + plan.Take.Value);
                }
            }
            else if (!plan.PaginationBeforePredicates && (plan.Skip.HasValue || plan.Take.HasValue))
            {
                // OPTIMIZATION: For .Where(...).Skip(N).Take(M), only collect Skip + Take indices
                // This enables early termination - we stop evaluating once we have enough matches
                int skipCount = plan.Skip ?? 0;
                int takeCount = plan.Take ?? int.MaxValue;
                
                // We need Skip + Take total matches to satisfy the query
                // Example: .Skip(1000).Take(100) only needs 1100 matches, not all matches
                if (takeCount < int.MaxValue)
                {
                    maxIndicesToCollect = skipCount + takeCount;
                }
            }
            
            var matchingIndices = SparseIndexCollector.CollectMatchingIndices(
                _recordBatch,
                plan.ColumnPredicates,
                _zoneMap,
                ParallelOptions,
                sparseEndRow,
                sparseStartRow,
                maxIndicesToCollect);
            
            return ExecuteWithSparseIndices<TResult>(plan, matchingIndices, resultType);
        }

        // DENSE PATH: Build selection bitmap using pooled bitfield (8x more memory efficient than bool[])
        
        // Determine if we should apply pagination before predicates
        // This happens when: Skip/Take is present, has predicates, AND pagination comes before predicates
        bool applyPaginationBeforePredicates = plan.ColumnPredicates.Count > 0 && plan.PaginationBeforePredicates;
        bool hasSkipBeforePredicates = applyPaginationBeforePredicates && plan.Skip.HasValue;
        bool hasTakeBeforePredicates = applyPaginationBeforePredicates && plan.Take.HasValue;
        
        // Calculate effective row range to evaluate
        int startRow = hasSkipBeforePredicates ? Math.Min(_count, plan.Skip!.Value) : 0;
        int endRow = _count;
        
        if (hasTakeBeforePredicates)
        {
            // .Take(N).Where(...) or .Skip(M).Take(N).Where(...)
            // Only evaluate rows from startRow to startRow + Take
            endRow = Math.Min(_count, startRow + plan.Take!.Value);
        }
        
        // Initialize bitmap: if pagination comes before predicates, only set affected range to true
        using var selection = SelectionBitmap.Create(_count, initialValue: false);
        
        if (applyPaginationBeforePredicates)
        {
            // For .Skip().Where() or .Take().Where() or .Skip().Take().Where()
            // Set only the relevant range of bits to true
            for (int i = startRow; i < endRow; i++)
            {
                selection.Set(i);
            }
        }
        else
        {
            // Normal case: set all bits to true
            for (int i = 0; i < _count; i++)
            {
                selection.Set(i);
            }
        }

        // Apply column predicates using parallel execution when beneficial
        // Pass row range to limit evaluation for pagination-before-predicates patterns
        int? maxRowToEvaluate = applyPaginationBeforePredicates ? (int?)endRow : null;
        
        if (plan.ColumnPredicates.Count > 0)
        {
            ParallelQueryExecutor.EvaluatePredicatesParallel(
                _recordBatch, 
                ref System.Runtime.CompilerServices.Unsafe.AsRef(in selection),
                plan.ColumnPredicates,
                ParallelOptions,
                _zoneMap,
                maxRowToEvaluate);
        }

        // Count selected rows using hardware popcount
        var selectedCount = selection.CountSet();

        // Handle grouped queries (GroupBy + Select with aggregates)
        if (plan.IsGroupedQuery)
        {
            return ExecuteGroupedQuery<TResult>(plan, ref System.Runtime.CompilerServices.Unsafe.AsRef(in selection));
        }

        // Handle simple aggregates (Sum, Average, Min, Max) directly on columns
        if (plan.SimpleAggregate is not null)
        {
            return ExecuteSimpleAggregate<TResult>(plan.SimpleAggregate, ref System.Runtime.CompilerServices.Unsafe.AsRef(in selection));
        }

        // IEnumerable<T> - return lazy enumeration using batched enumerator for better performance
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
            
            // Apply Skip and Take pagination
            // Pass flag indicating if pagination was already applied during evaluation
            selectedIndices = ApplyPagination(selectedIndices, plan, applyPaginationBeforePredicates);
            
            var enumerable = CreateBatchedEnumerable(selectedIndices);
            return (TResult)enumerable;
        }

        // Single element results (First, Single, etc.) - without predicates
        if (resultType == _elementType)
        {
            // Apply Skip for First operations if specified
            int skipCount = plan.Skip ?? 0;
            int itemsSeen = 0;
            
            foreach (var i in selection.GetSelectedIndices())
            {
                if (itemsSeen >= skipCount)
                {
                    return (TResult)_createItem(_recordBatch, i);
                }
                itemsSeen++;
            }
            
            // Check if this is an OrDefault variant
            if (expression is MethodCallExpression singleMethodCall && 
                (singleMethodCall.Method.Name == "FirstOrDefault" || singleMethodCall.Method.Name == "SingleOrDefault"))
            {
                return default!;
            }
            
            throw new InvalidOperationException("Sequence contains no elements.");
        }

        // Determine if we've already applied Take before predicates
        bool tookAlreadyApplied = plan.Take.HasValue && !plan.Skip.HasValue && 
                                  plan.ColumnPredicates.Count > 0 && plan.PaginationBeforePredicates;

        // Count - needs to account for Skip and Take
        // If we've already applied inner Take before predicates, don't apply it again
        // But DO apply outer Take after predicates
        if (resultType == typeof(int))
        {
            var count = selectedCount;
            if (!tookAlreadyApplied)
            {
                if (plan.Skip.HasValue)
                {
                    count = Math.Max(0, count - plan.Skip.Value);
                }
                if (plan.Take.HasValue)
                {
                    count = Math.Min(count, plan.Take.Value);
                }
            }
            
            // Apply outer Take (after predicates) if present
            if (plan.TakeAfterPredicates.HasValue)
            {
                count = Math.Min(count, plan.TakeAfterPredicates.Value);
            }
            
            return (TResult)(object)count;
        }

        // LongCount - needs to account for Skip and Take
        // If we've already applied inner Take before predicates, don't apply it again
        // But DO apply outer Take after predicates
        if (resultType == typeof(long))
        {
            var count = (long)selectedCount;
            if (!tookAlreadyApplied)
            {
                if (plan.Skip.HasValue)
                {
                    count = Math.Max(0L, count - plan.Skip.Value);
                }
                if (plan.Take.HasValue)
                {
                    count = Math.Min(count, plan.Take.Value);
                }
            }
            
            // Apply outer Take (after predicates) if present
            if (plan.TakeAfterPredicates.HasValue)
            {
                count = Math.Min(count, plan.TakeAfterPredicates.Value);
            }
            
            return (TResult)(object)count;
        }

        // Boolean results (Any, All) - without predicates or handled above
        if (resultType == typeof(bool))
        {
            if (expression is MethodCallExpression boolCall)
            {
                if (boolCall.Method.Name == "Any")
                {
                    return (TResult)(object)(selectedCount > 0);
                }
                if (boolCall.Method.Name == "All")
                {
                    return (TResult)(object)(selectedCount == _count);
                }
            }
        }

        throw new NotSupportedException($"Result type '{resultType}' is not supported.");
    }

    /// <summary>
    /// Executes a query using sparse index collection (for <5% selectivity).
    /// Avoids bitmap materialization overhead when very few rows match.
    /// </summary>
    private TResult ExecuteWithSparseIndices<TResult>(QueryPlan plan, List<int> matchingIndices, Type resultType)
    {
        var selectedCount = matchingIndices.Count;
        
        // Handle grouped queries (GroupBy + Select with aggregates)
        if (plan.IsGroupedQuery)
        {
            // Convert indices to bitmap for grouped query execution
            using var selection = SelectionBitmap.Create(_count, initialValue: false);
            foreach (var idx in matchingIndices)
            {
                selection.Set(idx);
            }
            return ExecuteGroupedQuery<TResult>(plan, ref System.Runtime.CompilerServices.Unsafe.AsRef(in selection));
        }

        // Handle simple aggregates (Sum, Average, Min, Max) directly on columns
        if (plan.SimpleAggregate is not null)
        {
            // For aggregates, convert to bitmap (aggregators expect bitmap interface)
            using var selection = SelectionBitmap.Create(_count, initialValue: false);
            foreach (var idx in matchingIndices)
            {
                selection.Set(idx);
            }
            return ExecuteSimpleAggregate<TResult>(plan.SimpleAggregate, ref System.Runtime.CompilerServices.Unsafe.AsRef(in selection));
        }

        // IEnumerable<T> - return lazy enumeration
        if (resultType.IsGenericType && 
            (resultType.GetGenericTypeDefinition() == typeof(IEnumerable<>) ||
             resultType.GetGenericTypeDefinition() == typeof(IQueryable<>)))
        {
            // Apply Skip and Take pagination
            // For sparse path, if we limited collection with row range, pagination is already applied
            bool paginationApplied = plan.PaginationBeforePredicates && plan.ColumnPredicates.Count > 0;
            var paginatedIndices = ApplyPagination(matchingIndices, plan, paginationApplied);
            var enumerable = CreateBatchedEnumerable(paginatedIndices);
            return (TResult)enumerable;
        }

        // Single element results (First, Single, etc.)
        if (resultType == _elementType)
        {
            int skipCount = plan.Skip ?? 0;
            if (skipCount >= selectedCount)
                throw new InvalidOperationException("Sequence contains no elements.");
            return (TResult)_createItem(_recordBatch, matchingIndices[skipCount]);
        }

        // Count - needs to account for Skip and Take
        if (resultType == typeof(int))
        {
            var count = selectedCount;
            if (plan.Skip.HasValue)
            {
                count = Math.Max(0, count - plan.Skip.Value);
            }
            if (plan.Take.HasValue)
            {
                count = Math.Min(count, plan.Take.Value);
            }
            // Apply outer Take (after predicates) if present
            if (plan.TakeAfterPredicates.HasValue)
            {
                count = Math.Min(count, plan.TakeAfterPredicates.Value);
            }
            return (TResult)(object)count;
        }

        // LongCount - needs to account for Skip and Take
        if (resultType == typeof(long))
        {
            var count = (long)selectedCount;
            if (plan.Skip.HasValue)
            {
                count = Math.Max(0L, count - plan.Skip.Value);
            }
            if (plan.Take.HasValue)
            {
                count = Math.Min(count, plan.Take.Value);
            }
            // Apply outer Take (after predicates) if present
            if (plan.TakeAfterPredicates.HasValue)
            {
                count = Math.Min(count, plan.TakeAfterPredicates.Value);
            }
            return (TResult)(object)count;
        }

        // Boolean results (Any, All)
        if (resultType == typeof(bool))
        {
            // For sparse path, if we got here, we have matching indices
            return (TResult)(object)(selectedCount > 0);
        }

        throw new NotSupportedException($"Result type '{resultType}' is not supported in sparse execution path.");
    }

    /// <summary>
    /// Executes short-circuit operations using streaming predicate evaluation.
    /// Stops as soon as the result can be determined without processing all rows.
    /// </summary>
    private TResult ExecuteShortCircuit<TResult>(QueryPlan plan, string operation)
    {
        var chunkSize = ParallelOptions?.ChunkSize ?? 16_384;
        
        // Calculate effective row range for pagination-before-predicates patterns
        int? maxRowToEvaluate = null;
        if (plan.PaginationBeforePredicates && plan.ColumnPredicates.Count > 0)
        {
            int startRow = plan.Skip ?? 0;
            int endRow = _count;
            if (plan.Take.HasValue)
            {
                endRow = Math.Min(_count, startRow + plan.Take.Value);
            }
            maxRowToEvaluate = endRow;
        }

        switch (operation)
        {
            case "Any":
                // Any(): return true as soon as we find one match
                var anyResult = StreamingPredicateEvaluator.Any(
                    _recordBatch, plan.ColumnPredicates, _zoneMap, chunkSize, maxRowToEvaluate);
                return (TResult)(object)anyResult;

            case "First":
            case "Single":
                // First()/Single(): find first matching row
                var firstIdx = StreamingPredicateEvaluator.FindFirst(
                    _recordBatch, plan.ColumnPredicates, _zoneMap, chunkSize, maxRowToEvaluate);
                if (firstIdx < 0)
                {
                    throw new InvalidOperationException("Sequence contains no matching elements.");
                }
                return (TResult)_createItem(_recordBatch, firstIdx);

            case "FirstOrDefault":
            case "SingleOrDefault":
                // FirstOrDefault(): find first matching row, or return default
                var firstOrDefaultIdx = StreamingPredicateEvaluator.FindFirst(
                    _recordBatch, plan.ColumnPredicates, _zoneMap, chunkSize, maxRowToEvaluate);
                if (firstOrDefaultIdx < 0)
                {
                    return default!;
                }
                return (TResult)_createItem(_recordBatch, firstOrDefaultIdx);

            default:
                throw new NotSupportedException($"Short-circuit operation '{operation}' is not supported.");
        }
    }

    private TResult ExecuteFusedAggregate<TResult>(QueryPlan plan)
    {
        var result = FusedAggregator.ExecuteFused(
            _recordBatch,
            plan.ColumnPredicates,
            plan.SimpleAggregate!,
            _columnIndexMap,
            ParallelOptions,
            _zoneMap);

        return (TResult)result;
    }


    private TResult ExecuteSimpleAggregate<TResult>(SimpleAggregateOperation aggregate, ref SelectionBitmap selection)
    {
        // For Average, Min, Max operations on empty selections, throw immediately
        // (Sum should return 0 for empty, but Average/Min/Max should throw)
        var selectedCount = selection.CountSet();
        if (selectedCount == 0)
        {
            if (aggregate.Operation == AggregationOperation.Average)
            {
                throw new InvalidOperationException("Sequence contains no elements");
            }
            if (aggregate.Operation == AggregationOperation.Min || aggregate.Operation == AggregationOperation.Max)
            {
                throw new InvalidOperationException("Sequence contains no elements");
            }
            // Sum returns 0 for empty (default value)
        }
        
        // Find the column by name
        var columnIndex = _columnIndexMap.TryGetValue(aggregate.ColumnName!, out var idx) 
            ? idx 
            : throw new InvalidOperationException($"Column '{aggregate.ColumnName}' not found.");
        
        var column = _recordBatch.Column(columnIndex);

        // Use parallel aggregator when enabled
        var result = aggregate.Operation switch
        {
            AggregationOperation.Sum => ParallelAggregator.ExecuteSumParallel(column, ref selection, aggregate.ResultType, ParallelOptions),
            AggregationOperation.Average => ParallelAggregator.ExecuteAverageParallel(column, ref selection, aggregate.ResultType, ParallelOptions),
            AggregationOperation.Min => ParallelAggregator.ExecuteMinParallel(column, ref selection, aggregate.ResultType, ParallelOptions),
            AggregationOperation.Max => ParallelAggregator.ExecuteMaxParallel(column, ref selection, aggregate.ResultType, ParallelOptions),
            _ => throw new NotSupportedException($"Aggregate operation {aggregate.Operation} is not supported.")
        };

        return (TResult)result;
    }

    private TResult ExecuteGroupedQuery<TResult>(QueryPlan plan, ref SelectionBitmap selection)
    {
        // Get the key column
        var keyColumnIndex = _columnIndexMap.TryGetValue(plan.GroupByColumn!, out var idx)
            ? idx
            : throw new InvalidOperationException($"Group key column '{plan.GroupByColumn}' not found.");
        
        var keyColumn = _recordBatch.Column(keyColumnIndex);

        // Execute the grouped query using reflection to handle the key type
        var method = typeof(ArrowQueryProvider)
            .GetMethod(nameof(ExecuteGroupedQueryTyped), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
            .MakeGenericMethod(plan.GroupByKeyType!, typeof(TResult));

        return (TResult)method.Invoke(this, [plan, keyColumn, selection])!;
    }

    private TResult ExecuteGroupedQueryTyped<TKey, TResult>(QueryPlan plan, IArrowArray keyColumn, SelectionBitmap selection) 
        where TKey : notnull
    {
        // Execute grouped aggregation
        var groupedResults = GroupedColumnAggregator.ExecuteGroupedQuery<TKey>(
            keyColumn,
            _recordBatch,
            ref selection,
            plan.Aggregations,
            _columnIndexMap);

        // Build result objects
        var resultType = typeof(TResult);
        
        // Handle Dictionary<TKey, TValue> result (from ToDictionary)
        if (resultType.IsGenericType && 
            resultType.GetGenericTypeDefinition() == typeof(Dictionary<,>))
        {
            var dictKeyType = resultType.GetGenericArguments()[0];
            var dictValueType = resultType.GetGenericArguments()[1];
            
            // Verify the key types match
            if (dictKeyType != typeof(TKey))
            {
                throw new InvalidOperationException(
                    $"Dictionary key type '{dictKeyType}' does not match group key type '{typeof(TKey)}'.");
            }

            return BuildDictionaryFromGroups<TKey, TResult>(groupedResults, dictValueType, plan);
        }

        // Handle IEnumerable<SomeProjectionType>
        if (resultType.IsGenericType && 
            (resultType.GetGenericTypeDefinition() == typeof(IEnumerable<>) ||
             resultType.GetGenericTypeDefinition() == typeof(IQueryable<>)))
        {
            var elementType = resultType.GetGenericArguments()[0];
            var results = CreateGroupedResultObjects<TKey>(groupedResults, elementType, plan.GroupByKeyResultPropertyName);
            return (TResult)results;
        }

        throw new NotSupportedException($"GroupBy result type '{resultType}' is not supported. Use .ToList(), .ToDictionary(), or enumerate the results.");
    }

    private static TResult BuildDictionaryFromGroups<TKey, TResult>(
        List<GroupedResult<TKey>> groupedResults,
        Type valueType,
        QueryPlan plan) where TKey : notnull
    {
        // Get the aggregation descriptor for the value
        var valueAggregation = plan.ToDictionaryValueAggregation
            ?? (plan.Aggregations.Count > 0 ? plan.Aggregations[0] : null)
            ?? throw new InvalidOperationException("No aggregation found for ToDictionary value.");

        // Create the dictionary using reflection for the value type
        var dictType = typeof(Dictionary<,>).MakeGenericType(typeof(TKey), valueType);
        var dict = (System.Collections.IDictionary)Activator.CreateInstance(dictType, groupedResults.Count)!;

        foreach (var group in groupedResults)
        {
            // Get the aggregated value from the group
            object? value = null;
            
            if (group.AggregateValues.TryGetValue(valueAggregation.ResultPropertyName, out var aggValue))
            {
                value = Convert.ChangeType(aggValue, valueType);
            }
            else if (group.AggregateValues.Count == 1)
            {
                // Fallback: if there's only one aggregate, use it
                value = Convert.ChangeType(group.AggregateValues.Values.First(), valueType);
            }
            else
            {
                throw new InvalidOperationException(
                    $"Could not find aggregate value '{valueAggregation.ResultPropertyName}' in group results.");
            }

            dict.Add(group.Key, value!);
        }

        return (TResult)dict;
    }




    private static object CreateGroupedResultObjects<TKey>(
        List<GroupedResult<TKey>> groupedResults,
        Type elementType,
        string keyPropertyName) where TKey : notnull
    {
        // Create a list of the result type
        var listType = typeof(List<>).MakeGenericType(elementType);
        var list = (System.Collections.IList)Activator.CreateInstance(listType, groupedResults.Count)!;

        // Check if it's an anonymous type (has constructor parameters matching properties)
        var constructor = elementType.GetConstructors().FirstOrDefault();
        var properties = elementType.GetProperties();

        foreach (var group in groupedResults)
        {
            object resultItem;

            if (constructor is not null && constructor.GetParameters().Length > 0)
            {
                // Anonymous type - use constructor
                var ctorParams = constructor.GetParameters();
                var args = new object?[ctorParams.Length];

                for (int i = 0; i < ctorParams.Length; i++)
                {
                    var paramName = ctorParams[i].Name!;
                    if (paramName.Equals(keyPropertyName, StringComparison.OrdinalIgnoreCase))
                    {
                        args[i] = group.Key;
                    }
                    else if (group.AggregateValues.TryGetValue(paramName, out var value))
                    {
                        args[i] = Convert.ChangeType(value, ctorParams[i].ParameterType);
                    }
                }

                resultItem = constructor.Invoke(args);
            }
            else
            {
                // Regular type with property setters
                resultItem = Activator.CreateInstance(elementType)!;

                foreach (var prop in properties)
                {
                    if (prop.Name.Equals(keyPropertyName, StringComparison.OrdinalIgnoreCase) && prop.CanWrite)
                    {
                        prop.SetValue(resultItem, group.Key);
                    }
                    else if (group.AggregateValues.TryGetValue(prop.Name, out var value) && prop.CanWrite)
                    {
                        prop.SetValue(resultItem, Convert.ChangeType(value, prop.PropertyType));
                    }
                }
            }

            list.Add(resultItem);
        }

        return list;
    }

    /// <summary>
    /// Creates a batched enumerable for lazy enumeration with improved cache locality.
    /// </summary>
    private object CreateBatchedEnumerable(List<int> selectedIndices)
    {
        var method = typeof(ArrowQueryProvider)
            .GetMethod(nameof(CreateBatchedEnumerableTyped), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
            .MakeGenericMethod(_elementType);
        return method.Invoke(this, [selectedIndices])!;
    }

    private MaterializedResultCollection<T> CreateBatchedEnumerableTyped<T>(List<int> selectedIndices)
    {
        T CreateItemFunc(RecordBatch batch, int index) => (T)_createItem(batch, index);
        return new MaterializedResultCollection<T>(_recordBatch, selectedIndices, CreateItemFunc, ParallelOptions);
    }

    /// <summary>
    /// Executes a query and returns results as an array using pooled materialization.
    /// This is the most efficient materialization path - direct array allocation, zero List overhead.
    /// </summary>
    internal T[] ExecuteToArray<T>(Expression expression)
    {
        var plan = AnalyzeExpression(expression);

        if (!plan.IsFullyOptimized && StrictMode)
        {
            throw new NotSupportedException(
                $"Query contains operations that cannot be optimized: {plan.UnsupportedReason}. " +
                $"Set ArrowQueryProvider.StrictMode = false to allow fallback materialization.");
        }

        // Build selection bitmap
        using var selection = SelectionBitmap.Create(_count, initialValue: true);

        // Apply column predicates
        if (plan.ColumnPredicates.Count > 0)
        {
            ParallelQueryExecutor.EvaluatePredicatesParallel(
                _recordBatch,
                ref System.Runtime.CompilerServices.Unsafe.AsRef(in selection),
                plan.ColumnPredicates,
                ParallelOptions,
                _zoneMap);
        }

        // Get selected indices
        var selectedCount = selection.CountSet();
        var selectedIndices = new List<int>(selectedCount);
        foreach (var idx in selection.GetSelectedIndices())
        {
            selectedIndices.Add(idx);
        }

        // Use pooled materialization
        T CreateItemFunc(RecordBatch batch, int index) => (T)_createItem(batch, index);
        return PooledBatchMaterializer.MaterializeToArray(_recordBatch, selectedIndices, CreateItemFunc, ParallelOptions);
    }

    /// <summary>
    /// Executes a query and returns the selected row indices without materializing objects.
    /// Zero-allocation path for advanced scenarios.
    /// </summary>
    internal int[] ExecuteToIndices<T>(Expression expression)
    {
        var plan = AnalyzeExpression(expression);

        if (!plan.IsFullyOptimized && StrictMode)
        {
            throw new NotSupportedException(
                $"Query contains operations that cannot be optimized: {plan.UnsupportedReason}. " +
                $"Set ArrowQueryProvider.StrictMode = false to allow fallback materialization.");
        }

        // Build selection bitmap
        using var selection = SelectionBitmap.Create(_count, initialValue: true);

        // Apply column predicates
        if (plan.ColumnPredicates.Count > 0)
        {
            ParallelQueryExecutor.EvaluatePredicatesParallel(
                _recordBatch,
                ref System.Runtime.CompilerServices.Unsafe.AsRef(in selection),
                plan.ColumnPredicates,
                ParallelOptions,
                _zoneMap);
        }

        // Get selected indices directly (minimal allocation)
        var selectedCount = selection.CountSet();
        var indices = new int[selectedCount];
        int pos = 0;
        foreach (var idx in selection.GetSelectedIndices())
        {
            indices[pos++] = idx;
        }

        return indices;
    }

    /// <summary>
    /// Legacy enumeration methods kept for compatibility (though batched version is used by default).
    /// </summary>
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

    /// <summary>
    /// Applies Skip and Take pagination to a list of selected indices.
    /// Returns a new list with pagination applied, or the original list if no pagination is specified.
    /// </summary>
    /// <param name="alreadyApplied">True if inner pagination was already applied during evaluation (PaginationBeforePredicates)</param>
    private static List<int> ApplyPagination(List<int> selectedIndices, QueryPlan plan, bool alreadyApplied = false)
    {
        // If inner pagination (Take before predicates) was already applied during evaluation,
        // we don't re-apply it. But we DO apply outer Take (after predicates) if present.
        if (alreadyApplied && plan.PaginationBeforePredicates)
        {
            // Inner Take/Skip already applied during evaluation
            // Apply outer Take if present: .Take(200).Where(...).Take(10)
            if (plan.TakeAfterPredicates.HasValue)
            {
                int outerTake = plan.TakeAfterPredicates.Value;
                if (outerTake >= selectedIndices.Count)
                {
                    return selectedIndices;
                }
                return [.. selectedIndices.Take(outerTake)];
            }
            return selectedIndices;
        }
        
        // Normal pagination path (pagination after predicates)
        if (!plan.Skip.HasValue && !plan.Take.HasValue && !plan.TakeAfterPredicates.HasValue)
        {
            // No pagination - return original list
            return selectedIndices;
        }

        int skip = plan.Skip ?? 0;
        int take = plan.Take ?? int.MaxValue;
        
        // Apply outer Take if it's more restrictive
        if (plan.TakeAfterPredicates.HasValue)
        {
            take = Math.Min(take, plan.TakeAfterPredicates.Value);
        }

        // Clamp skip to valid range
        if (skip >= selectedIndices.Count)
        {
            // Skip all elements
            return [];
        }

        // Calculate actual count to take
        int actualTake = Math.Min(take, selectedIndices.Count - skip);

        // Create paginated list efficiently
        var paginated = new List<int>(actualTake);
        for (int i = skip; i < skip + actualTake; i++)
        {
            paginated.Add(selectedIndices[i]);
        }

        return paginated;
    }

    /// <summary>
    /// Executes multiple aggregates in a single pass over the data.
    /// </summary>
    internal TResult ExecuteMultiAggregate<T, TResult>(
        Expression queryExpression,
        System.Linq.Expressions.Expression<Func<AggregateBuilder<T>, TResult>> aggregateSelector)
    {
        // Analyze the query to get predicates
        var plan = AnalyzeExpression(queryExpression);

        if (!plan.IsFullyOptimized && StrictMode)
        {
            throw new NotSupportedException(
                $"Query contains operations that cannot be optimized: {plan.UnsupportedReason}. " +
                $"Set ArrowQueryProvider.StrictMode = false to allow fallback materialization, " +
                $"or modify the query to use supported operations.");
        }

        // Build selection bitmap
        using var selection = SelectionBitmap.Create(_count, initialValue: true);

        // Apply column predicates using parallel execution when beneficial
        if (plan.ColumnPredicates.Count > 0)
        {
            ParallelQueryExecutor.EvaluatePredicatesParallel(
                _recordBatch,
                ref System.Runtime.CompilerServices.Unsafe.AsRef(in selection),
                plan.ColumnPredicates,
                ParallelOptions,
                _zoneMap);
        }

        // Parse the aggregate selector to extract aggregations
        var aggregations = ParseAggregateSelector(aggregateSelector);

        // Execute all aggregates in a single pass
        var aggregateResults = MultiAggregateExecutor.Execute(
            _recordBatch,
            ref System.Runtime.CompilerServices.Unsafe.AsRef(in selection),
            aggregations,
            _columnIndexMap);

        // Build the result object
        return BuildAggregateResult<TResult>(aggregateSelector, aggregateResults);
    }

    private static List<AggregationDescriptor> ParseAggregateSelector<T, TResult>(
        System.Linq.Expressions.Expression<Func<AggregateBuilder<T>, TResult>> selector)
    {
        // We need to analyze the expression to extract the aggregate calls
        // The expression is like: agg => new ResultType { Prop1 = agg.Sum(...), Prop2 = agg.Count(), ... }
        
        var aggregations = new List<AggregationDescriptor>();
        var body = selector.Body;

        if (body is MemberInitExpression memberInit)
        {
            foreach (var binding in memberInit.Bindings)
            {
                if (binding is MemberAssignment assignment)
                {
                    var agg = ParseAggregateCall(assignment.Expression, assignment.Member.Name);
                    if (agg is not null)
                    {
                        aggregations.Add(agg);
                    }
                }
            }
        }
        else if (body is NewExpression newExpr && newExpr.Members is not null)
        {
            for (int i = 0; i < newExpr.Arguments.Count; i++)
            {
                var memberName = newExpr.Members[i].Name;
                var agg = ParseAggregateCall(newExpr.Arguments[i], memberName);
                if (agg is not null)
                {
                    aggregations.Add(agg);
                }
            }
        }

        return aggregations;
    }

    private static AggregationDescriptor? ParseAggregateCall(Expression expression, string resultPropertyName)
    {
        if (expression is not MethodCallExpression methodCall)
            return null;

        var methodName = methodCall.Method.Name;
        string? columnName = null;

        // Extract column name from selector argument if present
        if (methodCall.Arguments.Count >= 1 && methodName != "Count" && methodName != "LongCount")
        {
            var selectorArg = methodCall.Arguments[0];
            if (selectorArg is UnaryExpression unary && unary.Operand is LambdaExpression lambda)
            {
                columnName = ExtractColumnNameFromLambda(lambda);
            }
            else if (selectorArg is LambdaExpression directLambda)
            {
                columnName = ExtractColumnNameFromLambda(directLambda);
            }
        }

        var operation = methodName switch
        {
            "Sum" => AggregationOperation.Sum,
            "Average" => AggregationOperation.Average,
            "Min" => AggregationOperation.Min,
            "Max" => AggregationOperation.Max,
            "Count" => AggregationOperation.Count,
            "LongCount" => AggregationOperation.LongCount,
            _ => (AggregationOperation?)null
        };

        if (operation is null)
            return null;

        return new AggregationDescriptor
        {
            Operation = operation.Value,
            ColumnName = columnName,
            ResultPropertyName = resultPropertyName
        };
    }

    private static string? ExtractColumnNameFromLambda(LambdaExpression lambda)
    {
        if (lambda.Body is MemberExpression memberExpr)
        {
            return memberExpr.Member.Name;
        }
        
        if (lambda.Body is UnaryExpression unary && unary.Operand is MemberExpression innerMember)
        {
            return innerMember.Member.Name;
        }

        return null;
    }

    private static TResult BuildAggregateResult<TResult>(
        LambdaExpression selector,
        Dictionary<string, object> aggregateResults)
    {
        var resultType = typeof(TResult);
        var body = selector.Body;

        if (body is MemberInitExpression memberInit)
        {
            var result = Activator.CreateInstance(resultType)!;
            
            foreach (var binding in memberInit.Bindings)
            {
                if (binding is MemberAssignment assignment)
                {
                    var propertyName = assignment.Member.Name;
                    if (aggregateResults.TryGetValue(propertyName, out var value))
                    {
                        var prop = resultType.GetProperty(propertyName);
                        if (prop is not null && prop.CanWrite)
                        {
                            prop.SetValue(result, Convert.ChangeType(value, prop.PropertyType));
                        }
                    }
                }
            }

            return (TResult)result;
        }
        else if (body is NewExpression newExpr && newExpr.Members is not null)
        {
            // Anonymous type - use constructor
            var constructor = resultType.GetConstructors().First();
            var ctorParams = constructor.GetParameters();
            var args = new object?[ctorParams.Length];

            for (int i = 0; i < ctorParams.Length; i++)
            {
                var paramName = newExpr.Members[i].Name;
                if (aggregateResults.TryGetValue(paramName, out var value))
                {
                    args[i] = Convert.ChangeType(value, ctorParams[i].ParameterType);
                }
            }

            return (TResult)constructor.Invoke(args);
        }

        throw new NotSupportedException("Aggregate selector must be an object initializer or anonymous type.");
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
        // OrderBy, OrderByDescending, ThenBy, ThenByDescending are NOT supported - they require fallback
        "Sum", "Average", "Min", "Max", "GroupBy", "ToDictionary"
    ];

    private static readonly HashSet<string> AggregateMethods =
    [
        "Sum", "Average", "Min", "Max"
    ];


    private string? _groupByColumn;
    private Type? _groupByKeyType;
    private string _groupByKeyResultPropertyName = "Key"; // Default to "Key"
    private readonly List<AggregationDescriptor> _aggregations = [];
    private bool _insideGroupByProjection; // Flag to allow Enumerable methods inside GroupBy projection
    
    // ToDictionary support
    private bool _isToDictionaryQuery;
    private AggregationDescriptor? _toDictionaryValueAggregation;

    // Pagination support
    private int? _skip;
    private int? _takeBeforePredicates;  // Inner Take: .Take(N).Where(...) - limits evaluation
    private int? _takeAfterPredicates;   // Outer Take: .Where(...).Take(N) - limits results
    private bool _paginationBeforePredicates = false; // Default: pagination comes after predicates
    private bool _seenPredicate = false; // Track if we've seen a Where clause yet

    public QueryPlan Analyze(Expression expression)
    {
        Visit(expression);

        // Combine inner and outer Takes for backward compatibility
        // If both exist, Take represents the inner (before predicates) one
        int? take = _takeBeforePredicates ?? _takeAfterPredicates;

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
            GroupByColumn = _groupByColumn,
            GroupByKeyType = _groupByKeyType,
            GroupByKeyResultPropertyName = _groupByKeyResultPropertyName,
            Aggregations = _aggregations,
            IsToDictionaryQuery = _isToDictionaryQuery,
            ToDictionaryValueAggregation = _toDictionaryValueAggregation,
            EstimatedSelectivity = EstimateSelectivity(),
            Skip = _skip,
            Take = take,
            TakeAfterPredicates = _takeAfterPredicates,  // NEW: Track outer Take separately
            PaginationBeforePredicates = _paginationBeforePredicates
        };
    }



    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        var methodName = node.Method.Name;
        var declaringType = node.Method.DeclaringType?.FullName ?? "";

        // Handle ArrowQueryExtensions.ToDictionary
        if (declaringType == "FrozenArrow.Query.ArrowQueryExtensions" && methodName == "ToDictionary")
        {
            // Check if the source is a GroupBy call
            if (node.Arguments.Count >= 3)
            {
                var source = node.Arguments[0];
                if (IsGroupByCall(source))
                {
                    // First analyze the GroupBy (if not already done)
                    if (_groupByColumn is null)
                    {
                        AnalyzeGroupByFromSource(source);
                    }
                    // Then analyze the ToDictionary selectors
                    AnalyzeToDictionaryAfterGroupBy(node);
                    // Visit only the underlying source (skip GroupBy + ToDictionary lambdas)
                    VisitUnderlyingSource(source);
                    return node; // Don't call base.VisitMethodCall
                }
            }
        }

        // Allow Enumerable methods inside GroupBy projection (g.Count(), g.Sum(), etc.)
        if (declaringType == "System.Linq.Enumerable" && !_insideGroupByProjection)
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
                _seenPredicate = true; // Mark that we've encountered a predicate
                var predicateArg = node.Arguments[1];
                if (predicateArg is UnaryExpression unary && unary.Operand is LambdaExpression lambda)
                {
                    AnalyzeWherePredicate(lambda);
                }
            }
            
            // Process Count/LongCount with predicate argument
            // .Count(predicate) is syntactic sugar for .Where(predicate).Count()
            if ((methodName == "Count" || methodName == "LongCount") && node.Arguments.Count >= 2)
            {
                _seenPredicate = true; // Mark that we've encountered a predicate
                var predicateArg = node.Arguments[1];
                if (predicateArg is UnaryExpression unary && unary.Operand is LambdaExpression lambda)
                {
                    AnalyzeWherePredicate(lambda);
                }
                else if (predicateArg is LambdaExpression directLambda)
                {
                    AnalyzeWherePredicate(directLambda);
                }
            }

            // Check if this Select follows a GroupBy (for grouped aggregation)
            if (methodName == "Select" && node.Arguments.Count >= 2)
            {
                // Check if the source is a GroupBy call
                var source = node.Arguments[0];
                if (IsGroupByCall(source))
                {
                    // First analyze the GroupBy (if not already done)
                    if (_groupByColumn is null)
                    {
                        AnalyzeGroupByFromSource(source);
                    }
                    // Then analyze the projection
                    AnalyzeGroupByProjection(node);
                    // Visit only the underlying source (skip GroupBy + Select lambdas)
                    VisitUnderlyingSource(source);
                    return node; // Don't call base.VisitMethodCall
                }
            }

            // Process GroupBy (when not followed by Select - rare)
            if (methodName == "GroupBy" && node.Arguments.Count >= 2)
            {
                AnalyzeGroupBy(node);
            }

            // Process aggregate methods (Sum, Average, Min, Max) - only for non-grouped queries
            if (AggregateMethods.Contains(methodName) && _groupByColumn is null)
            {
                AnalyzeAggregateMethod(node, methodName);
            }

            // Process Take
            if (methodName == "Take" && node.Arguments.Count >= 2)
            {
                if (node.Arguments[1] is ConstantExpression takeConst && takeConst.Value is int takeValue)
                {
                    // Distinguish between inner Take (before predicates) and outer Take (after predicates)
                    // Expression tree is visited outside-in, so:
                    // - If _seenPredicate is FALSE: this is an OUTER Take (comes after Where in execution)
                    // - If _seenPredicate is TRUE: this is an INNER Take (comes before Where in execution)
                    
                    if (_seenPredicate)
                    {
                        // Inner Take: .Take(N).Where(...) - limit evaluation
                        _takeBeforePredicates = takeValue;
                        _paginationBeforePredicates = true;
                    }
                    else
                    {
                        // Outer Take: .Where(...).Take(N) - limit results
                        _takeAfterPredicates = takeValue;
                    }
                }
            }

            // Process Skip
            if (methodName == "Skip" && node.Arguments.Count >= 2)
            {
                if (node.Arguments[1] is ConstantExpression skipConst && skipConst.Value is int skipValue)
                {
                    _skip = skipValue;
                    // If we HAVE seen a predicate (Where is on the outside, Skip is inner/earlier)
                    // then pagination comes BEFORE predicates logically: .Skip(N).Where(...)
                    if (_seenPredicate)
                    {
                        _paginationBeforePredicates = true;
                    }
                }
            }
        }

        return base.VisitMethodCall(node);
    }

    private static bool IsGroupByCall(Expression expression)
    {
        if (expression is MethodCallExpression methodCall)
        {
            return methodCall.Method.Name == "GroupBy" && 
                   methodCall.Method.DeclaringType?.FullName == "System.Linq.Queryable";
        }
        return false;
    }

    private void AnalyzeGroupByFromSource(Expression source)
    {
        if (source is MethodCallExpression groupByCall)
        {
            AnalyzeGroupBy(groupByCall);
        }
    }

    private void VisitUnderlyingSource(Expression source)
    {
        // source is GroupBy(innerSource, keySelector)
        // We want to visit innerSource
        if (source is MethodCallExpression groupByCall && groupByCall.Arguments.Count > 0)
        {
            Visit(groupByCall.Arguments[0]); // Visit the source of GroupBy
        }
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

    private void AnalyzeGroupBy(MethodCallExpression node)
    {
        // Extract key selector: GroupBy(x => x.Category)
        var keySelectorArg = node.Arguments[1];
        if (keySelectorArg is UnaryExpression unary && unary.Operand is LambdaExpression lambda)
        {
            var columnName = ExtractColumnNameFromSelector(lambda);
            if (columnName is not null)
            {
                _groupByColumn = columnName;
                _groupByKeyType = lambda.ReturnType;
                _columnsAccessed.Add(columnName);
            }
            else
            {
                _hasUnsupportedPatterns = true;
                _unsupportedReasons.Add("GroupBy key selector must be a simple property access (x => x.Property).");
            }
        }
    }

    private void AnalyzeToDictionaryAfterGroupBy(MethodCallExpression node)
    {
        // Parse: .GroupBy(x => x.Category).ToDictionary(g => g.Key, g => g.Count())
        // or:    .GroupBy(x => x.Category).ToDictionary(g => g.Key, g => g.Sum(x => x.Salary))
        
        _isToDictionaryQuery = true;
        _insideGroupByProjection = true;

        try
        {
            // Arguments[0] is the source (GroupBy result)
            // Arguments[1] is the key selector: g => g.Key
            // Arguments[2] is the element/value selector: g => g.Count() or g => g.Sum(x => x.Salary)

            // Validate key selector is g => g.Key
            var keySelectorArg = node.Arguments[1];
            if (!ValidateToDictionaryKeySelector(keySelectorArg))
            {
                _hasUnsupportedPatterns = true;
                _unsupportedReasons.Add("ToDictionary key selector must be 'g => g.Key'.");
                return;
            }

            // Parse the element/value selector
            var elementSelectorArg = node.Arguments[2];
            var aggregation = ParseToDictionaryElementSelector(elementSelectorArg);
            
            if (aggregation is not null)
            {
                _toDictionaryValueAggregation = aggregation;
                _aggregations.Add(aggregation);
                
                if (aggregation.ColumnName is not null)
                {
                    _columnsAccessed.Add(aggregation.ColumnName);
                }
            }
            else
            {
                _hasUnsupportedPatterns = true;
                _unsupportedReasons.Add(
                    "ToDictionary element selector must be an aggregate: g.Count(), g.Sum(x => x.Col), " +
                    "g.Average(x => x.Col), g.Min(x => x.Col), or g.Max(x => x.Col).");
            }
        }
        finally
        {
            _insideGroupByProjection = false;
        }
    }

    private static bool ValidateToDictionaryKeySelector(Expression keySelectorArg)
    {
        // Expect: g => g.Key
        LambdaExpression? lambda = null;
        
        if (keySelectorArg is UnaryExpression unary && unary.Operand is LambdaExpression lambda1)
        {
            lambda = lambda1;
        }
        else if (keySelectorArg is LambdaExpression lambda2)
        {
            lambda = lambda2;
        }

        if (lambda is null)
            return false;

        // Check body is g.Key
        if (lambda.Body is MemberExpression memberExpr &&
            memberExpr.Expression == lambda.Parameters[0] &&
            memberExpr.Member.Name == "Key")
        {
            return true;
        }

        return false;
    }

    private AggregationDescriptor? ParseToDictionaryElementSelector(Expression elementSelectorArg)
    {
        // Expect: g => g.Count(), g => g.Sum(x => x.Salary), etc.
        LambdaExpression? lambda = null;
        
        if (elementSelectorArg is UnaryExpression unary && unary.Operand is LambdaExpression lambda1)
        {
            lambda = lambda1;
        }
        else if (elementSelectorArg is LambdaExpression lambda2)
        {
            lambda = lambda2;
        }

        if (lambda is null)
            return null;

        var body = lambda.Body;
        var groupParam = lambda.Parameters[0];

        // Check for g.Count() or g.LongCount()
        if (body is MethodCallExpression countCall)
        {
            if ((countCall.Method.Name == "Count" || countCall.Method.Name == "LongCount") &&
                countCall.Arguments.Count >= 1 &&
                IsGroupParameterAccess(countCall.Arguments[0], groupParam))
            {
                return new AggregationDescriptor
                {
                    Operation = countCall.Method.Name == "Count" 
                        ? AggregationOperation.Count 
                        : AggregationOperation.LongCount,
                    ColumnName = null,
                    ResultPropertyName = "Value" // Default for dictionary value
                };
            }

            // Check for g.Sum(x => x.Salary), g.Average(...), g.Min(...), g.Max(...)
            if (AggregateMethods.Contains(countCall.Method.Name) &&
                countCall.Arguments.Count >= 2 &&
                IsGroupParameterAccess(countCall.Arguments[0], groupParam))
            {
                var selectorArg = countCall.Arguments[1];
                LambdaExpression? aggLambda = null;

                if (selectorArg is UnaryExpression aggUnary && aggUnary.Operand is LambdaExpression aggLambda1)
                {
                    aggLambda = aggLambda1;
                }
                else if (selectorArg is LambdaExpression aggLambda2)
                {
                    aggLambda = aggLambda2;
                }

                if (aggLambda is not null)
                {
                    var columnName = ExtractColumnNameFromSelector(aggLambda);
                    if (columnName is not null)
                    {
                        var operation = countCall.Method.Name switch
                        {
                            "Sum" => AggregationOperation.Sum,
                            "Average" => AggregationOperation.Average,
                            "Min" => AggregationOperation.Min,
                            "Max" => AggregationOperation.Max,
                            _ => throw new NotSupportedException($"Unknown aggregate: {countCall.Method.Name}")
                        };

                        return new AggregationDescriptor
                        {
                            Operation = operation,
                            ColumnName = columnName,
                            ResultPropertyName = "Value" // Default for dictionary value
                        };
                    }
                }
            }
        }

        return null;
    }

    private static bool IsGroupParameterAccess(Expression expr, ParameterExpression groupParam)
    {
        return expr == groupParam ||
               (expr is UnaryExpression unary && unary.Operand == groupParam);
    }

    private void AnalyzeGroupByProjection(MethodCallExpression node)
    {
        // Set flag to allow Enumerable methods inside the projection
        _insideGroupByProjection = true;

        try
        {
            // Parse: .Select(g => new { g.Key, Total = g.Sum(x => x.Salary), ... })
            var selectorArg = node.Arguments[1];
            if (selectorArg is not UnaryExpression unary || unary.Operand is not LambdaExpression lambda)
                return;

            var body = lambda.Body;

            // Handle anonymous type: new { g.Key, Total = g.Sum(...) }
            if (body is NewExpression newExpr)
            {
                AnalyzeProjectionMembers(newExpr, lambda.Parameters[0]);
            }
            // Handle member init: new ResultType { Key = g.Key, Total = g.Sum(...) }
            else if (body is MemberInitExpression memberInit)
            {
                AnalyzeProjectionMemberInit(memberInit, lambda.Parameters[0]);
            }
            else
            {
                _hasUnsupportedPatterns = true;
                _unsupportedReasons.Add("GroupBy projection must be an anonymous type or object initializer.");
            }
        }
        finally
        {
            _insideGroupByProjection = false;
        }
    }

    private void AnalyzeProjectionMembers(NewExpression newExpr, ParameterExpression groupParam)
    {
        if (newExpr.Members is null) return;

        for (int i = 0; i < newExpr.Arguments.Count; i++)
        {
            var arg = newExpr.Arguments[i];
            var memberName = newExpr.Members[i].Name;

            AnalyzeProjectionMember(arg, memberName, groupParam);
        }
    }

    private void AnalyzeProjectionMemberInit(MemberInitExpression memberInit, ParameterExpression groupParam)
    {
        foreach (var binding in memberInit.Bindings)
        {
            if (binding is MemberAssignment assignment)
            {
                AnalyzeProjectionMember(assignment.Expression, binding.Member.Name, groupParam);
            }
        }
    }

    private void AnalyzeProjectionMember(Expression expression, string memberName, ParameterExpression groupParam)
    {
        // Check for g.Key
        if (expression is MemberExpression memberExpr && 
            memberExpr.Expression == groupParam && 
            memberExpr.Member.Name == "Key")
        {
            // Track the result property name for the key (e.g., "Category" in "Category = g.Key")
            _groupByKeyResultPropertyName = memberName;
            return;
        }

        // Check for g.Count()
        if (expression is MethodCallExpression countCall && 
            countCall.Method.Name == "Count" &&
            countCall.Arguments.Count >= 1 &&
            IsGroupParameter(countCall.Arguments[0], groupParam))
        {
            _aggregations.Add(new AggregationDescriptor
            {
                Operation = AggregationOperation.Count,
                ColumnName = null,
                ResultPropertyName = memberName
            });
            return;
        }

        // Check for g.LongCount()
        if (expression is MethodCallExpression longCountCall && 
            longCountCall.Method.Name == "LongCount" &&
            longCountCall.Arguments.Count >= 1 &&
            IsGroupParameter(longCountCall.Arguments[0], groupParam))
        {
            _aggregations.Add(new AggregationDescriptor
            {
                Operation = AggregationOperation.LongCount,
                ColumnName = null,
                ResultPropertyName = memberName
            });
            return;
        }

        // Check for g.Sum(x => x.Salary), g.Average(...), g.Min(...), g.Max(...)
        if (expression is MethodCallExpression aggCall && 
            AggregateMethods.Contains(aggCall.Method.Name) &&
            aggCall.Arguments.Count >= 2)
        {
            // The selector is the second argument
            var selectorArg = aggCall.Arguments[1];
            LambdaExpression? aggLambda = null;
            
            // Handle different wrapping: UnaryExpression or direct lambda
            if (selectorArg is UnaryExpression unary && unary.Operand is LambdaExpression lambda1)
            {
                aggLambda = lambda1;
            }
            else if (selectorArg is LambdaExpression lambda2)
            {
                aggLambda = lambda2;
            }
            
            if (aggLambda is not null)
            {
                var columnName = ExtractColumnNameFromSelector(aggLambda);
                if (columnName is not null)
                {
                    var operation = aggCall.Method.Name switch
                    {
                        "Sum" => AggregationOperation.Sum,
                        "Average" => AggregationOperation.Average,
                        "Min" => AggregationOperation.Min,
                        "Max" => AggregationOperation.Max,
                        _ => throw new NotSupportedException($"Unknown aggregate: {aggCall.Method.Name}")
                    };

                    _aggregations.Add(new AggregationDescriptor
                    {
                        Operation = operation,
                        ColumnName = columnName,
                        ResultPropertyName = memberName
                    });
                    _columnsAccessed.Add(columnName);
                    return;
                }
            }
        }

        // Unsupported projection member
        _hasUnsupportedPatterns = true;
        _unsupportedReasons.Add($"Unsupported projection member '{memberName}'. " +
            "Only g.Key, g.Count(), g.Sum(x => x.Col), g.Average(x => x.Col), g.Min(x => x.Col), g.Max(x => x.Col) are supported.");
    }

    private static bool IsGroupParameter(Expression expression, ParameterExpression groupParam)
    {
        return expression == groupParam || 
               (expression is UnaryExpression unary && unary.Operand == groupParam);
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
