using Apache.Arrow;
using System.Linq.Expressions;

namespace FrozenArrow.Query.LogicalPlan;

/// <summary>
/// Executes logical plans directly without converting to QueryPlan.
/// Phase 5: Direct execution without bridge.
/// Phase 9: Integrated compiled query execution.
/// </summary>
internal sealed partial class LogicalPlanExecutor(
    RecordBatch recordBatch,
    int count,
    Dictionary<string, int> columnIndexMap,
    Func<RecordBatch, int, object> createItem,
    ZoneMap? zoneMap,
    ParallelQueryOptions? parallelOptions,
    bool useCompiledQueries = false)
{
    private readonly RecordBatch _recordBatch = recordBatch ?? throw new ArgumentNullException(nameof(recordBatch));
    private readonly Dictionary<string, int> _columnIndexMap = columnIndexMap ?? throw new ArgumentNullException(nameof(columnIndexMap));
    private readonly Func<RecordBatch, int, object> _createItem = createItem ?? throw new ArgumentNullException(nameof(createItem));
    private readonly bool _useCompiledQueries = useCompiledQueries;
    private readonly Compilation.CompiledQueryExecutor? _compiledExecutor = useCompiledQueries 
        ? new Compilation.CompiledQueryExecutor(recordBatch, count) 
        : null;

    /// <summary>
    /// Executes a logical plan and returns results.
    /// </summary>
    public TResult Execute<TResult>(LogicalPlanNode plan)
    {
        // Pattern match on the plan type and execute accordingly
        return plan switch
        {
            ScanPlan scan => ExecuteScan<TResult>(scan),
            FilterPlan filter => ExecuteFilter<TResult>(filter),
            GroupByPlan groupBy => ExecuteGroupBy<TResult>(groupBy),
            AggregatePlan aggregate => ExecuteAggregate<TResult>(aggregate),
            LimitPlan limit => ExecuteLimit<TResult>(limit),
            OffsetPlan offset => ExecuteOffset<TResult>(offset),
            ProjectPlan project => ExecuteProject<TResult>(project),
            DistinctPlan distinct => ExecuteDistinct<TResult>(distinct),
            SortPlan sort => ExecuteSort<TResult>(sort),
            _ => throw new NotSupportedException($"Logical plan node type '{plan.GetType().Name}' is not yet supported for direct execution")
        };
    }

    private TResult ExecuteScan<TResult>(ScanPlan scan)
    {
        // Scan returns all rows
        // The result type determines how we materialize
        var resultType = typeof(TResult);

        if (resultType.IsGenericType)
        {
            var genericType = resultType.GetGenericTypeDefinition();
            
            // IEnumerable<T> or IQueryable<T>
            if (genericType == typeof(IEnumerable<>) || genericType == typeof(IQueryable<>))
            {
                var selectedIndices = Enumerable.Range(0, count).ToList();
                var enumerable = CreateBatchedEnumerable(selectedIndices);
                return (TResult)enumerable;
            }
        }

        throw new NotSupportedException($"Result type '{resultType}' not supported for ScanPlan");
    }

    private TResult ExecuteFilter<TResult>(FilterPlan filter)
    {
        // Phase 9: Use compiled execution if enabled
        if (_useCompiledQueries && _compiledExecutor != null && filter.Predicates.Count > 0)
        {
            return ExecuteFilterCompiled<TResult>(filter);
        }

        // Default: Interpreted execution (Phase 5)
        return ExecuteFilterInterpreted<TResult>(filter);
    }

    private TResult ExecuteFilterCompiled<TResult>(FilterPlan filter)
    {
        var resultType = typeof(TResult);

        // For Count() - use optimized compiled path
        if (resultType == typeof(int))
        {
            var count = _compiledExecutor!.ExecuteFilterCount(filter);
            return (TResult)(object)count;
        }

        // For other operations - get matching indices
        var selectedIndices = _compiledExecutor!.ExecuteFilter(filter);
        var selectedCount = selectedIndices.Count;

        if (resultType == typeof(long))
        {
            return (TResult)(object)(long)selectedCount;
        }

        if (resultType == typeof(bool))
        {
            return (TResult)(object)(selectedCount > 0);
        }

        if (resultType.IsGenericType)
        {
            var genericType = resultType.GetGenericTypeDefinition();
            
            if (genericType == typeof(IEnumerable<>) || genericType == typeof(IQueryable<>))
            {
                var enumerable = CreateBatchedEnumerable(selectedIndices);
                return (TResult)enumerable;
            }
        }

        // Single element (First, etc.)
        if (selectedIndices.Count > 0)
        {
            return (TResult)_createItem(_recordBatch, selectedIndices[0]);
        }
        
        throw new InvalidOperationException("Sequence contains no elements.");
    }

    private TResult ExecuteFilterInterpreted<TResult>(FilterPlan filter)
    {
        // Build selection bitmap from predicates
        using var selection = SelectionBitmap.Create(count, initialValue: true);

        if (filter.Predicates.Count > 0)
        {
            ParallelQueryExecutor.EvaluatePredicatesParallel(
                _recordBatch,
                ref System.Runtime.CompilerServices.Unsafe.AsRef(in selection),
                filter.Predicates,
                parallelOptions,
                zoneMap,
                null);
        }

        var selectedCount = selection.CountSet();
        var resultType = typeof(TResult);

        // Handle different result types
        if (resultType == typeof(int))
        {
            return (TResult)(object)selectedCount;
        }

        if (resultType == typeof(long))
        {
            return (TResult)(object)(long)selectedCount;
        }

        if (resultType == typeof(bool))
        {
            // Any - check if any rows selected
            return (TResult)(object)(selectedCount > 0);
        }

        if (resultType.IsGenericType)
        {
            var genericType = resultType.GetGenericTypeDefinition();
            
            if (genericType == typeof(IEnumerable<>) || genericType == typeof(IQueryable<>))
            {
                // Enumerate filtered results
                var selectedIndices = new List<int>(selectedCount);
                foreach (var idx in selection.GetSelectedIndices())
                {
                    selectedIndices.Add(idx);
                }
                
                var enumerable = CreateBatchedEnumerable(selectedIndices);
                return (TResult)enumerable;
            }
        }

        // Single element (First, etc.)
        var elementType = GetElementType(resultType);
        if (elementType != null)
        {
            foreach (var idx in selection.GetSelectedIndices())
            {
                return (TResult)_createItem(_recordBatch, idx);
            }
            
            throw new InvalidOperationException("Sequence contains no elements.");
        }

        throw new NotSupportedException($"Result type '{resultType}' not supported for FilterPlan");
    }

    private TResult ExecuteGroupBy<TResult>(GroupByPlan groupBy)
    {
        // First, apply any filters from the input plan
        var predicates = new List<ColumnPredicate>();
        var inputPlan = groupBy.Input;
        
        while (inputPlan is FilterPlan filterPlan)
        {
            predicates.AddRange(filterPlan.Predicates);
            inputPlan = filterPlan.Input;
        }

        // Build selection bitmap
        using var selection = SelectionBitmap.Create(count, initialValue: true);
        
        if (predicates.Count > 0)
        {
            ParallelQueryExecutor.EvaluatePredicatesParallel(
                _recordBatch,
                ref System.Runtime.CompilerServices.Unsafe.AsRef(in selection),
                predicates,
                parallelOptions,
                zoneMap,
                null);
        }

        // Get the key column
        var keyColumnIndex = _columnIndexMap.TryGetValue(groupBy.GroupByColumn, out var idx)
            ? idx
            : throw new InvalidOperationException($"Group key column '{groupBy.GroupByColumn}' not found.");
        
        var keyColumn = _recordBatch.Column(keyColumnIndex);

        // Execute grouped query using existing infrastructure
        return ExecuteGroupedQueryTyped<TResult>(groupBy, keyColumn, selection);
    }

    private TResult ExecuteGroupedQueryTyped<TResult>(GroupByPlan groupBy, IArrowArray keyColumn, SelectionBitmap selection)
    {
        // Use reflection to call the generic method with the correct key type
        var executeMethod = typeof(LogicalPlanExecutor)
            .GetMethod(nameof(ExecuteGroupedQueryTypedInternal), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
            .MakeGenericMethod(groupBy.GroupByKeyType, typeof(TResult));
        
        return (TResult)executeMethod.Invoke(this, [groupBy, keyColumn, selection])!;
    }

    private TResult ExecuteGroupedQueryTypedInternal<TKey, TResult>(GroupByPlan groupBy, IArrowArray keyColumn, SelectionBitmap selection)
        where TKey : notnull
    {
        // Use existing GroupedColumnAggregator
        var results = GroupedColumnAggregator.ExecuteGroupedQuery<TKey>(
            keyColumn,
            _recordBatch,
            ref System.Runtime.CompilerServices.Unsafe.AsRef(in selection),
            groupBy.Aggregations,
            _columnIndexMap);

        // The results are List<GroupedResult<TKey>>
        // Need to convert to TResult
        return (TResult)(object)results;
    }

    private TResult ExecuteAggregate<TResult>(AggregatePlan aggregate)
    {
        // Apply filters from input
        var predicates = new List<ColumnPredicate>();
        var inputPlan = aggregate.Input;
        
        while (inputPlan is FilterPlan filterPlan)
        {
            predicates.AddRange(filterPlan.Predicates);
            inputPlan = filterPlan.Input;
        }

        // Build selection bitmap
        using var selection = SelectionBitmap.Create(count, initialValue: true);
        
        if (predicates.Count > 0)
        {
            ParallelQueryExecutor.EvaluatePredicatesParallel(
                _recordBatch,
                ref System.Runtime.CompilerServices.Unsafe.AsRef(in selection),
                predicates,
                parallelOptions,
                zoneMap,
                null);
        }

        var selectedCount = selection.CountSet();

        // Handle Count without column
        if (aggregate.Operation == AggregationOperation.Count && aggregate.ColumnName == null)
        {
            if (typeof(TResult) == typeof(int))
            {
                return (TResult)(object)selectedCount;
            }
            if (typeof(TResult) == typeof(long))
            {
                return (TResult)(object)(long)selectedCount;
            }
        }

        // Handle other aggregations
        if (aggregate.ColumnName != null)
        {
            var columnIndex = _columnIndexMap.TryGetValue(aggregate.ColumnName, out var idx)
                ? idx
                : throw new InvalidOperationException($"Column '{aggregate.ColumnName}' not found.");
            
            var column = _recordBatch.Column(columnIndex);

            // Create a ref to the selection for the parallel aggregator
            ref var selectionRef = ref System.Runtime.CompilerServices.Unsafe.AsRef(in selection);

            var result = aggregate.Operation switch
            {
                AggregationOperation.Sum => ParallelAggregator.ExecuteSumParallel(column, ref selectionRef, aggregate.OutputType, parallelOptions),
                AggregationOperation.Average => ParallelAggregator.ExecuteAverageParallel(column, ref selectionRef, aggregate.OutputType, parallelOptions),
                AggregationOperation.Min => ParallelAggregator.ExecuteMinParallel(column, ref selectionRef, aggregate.OutputType, parallelOptions),
                AggregationOperation.Max => ParallelAggregator.ExecuteMaxParallel(column, ref selectionRef, aggregate.OutputType, parallelOptions),
                _ => throw new NotSupportedException($"Aggregate operation {aggregate.Operation} is not supported.")
            };

            return (TResult)result;
        }

        throw new NotSupportedException($"Aggregate plan with operation '{aggregate.Operation}' cannot be executed");
    }

    private TResult ExecuteLimit<TResult>(LimitPlan limit)
    {
        // Execute input first
        var inputResult = Execute<IEnumerable<object>>(limit.Input);
        
        // Apply limit
        var limited = inputResult.Take(limit.Count);
        
        // Convert to expected result type
        if (typeof(TResult).IsAssignableFrom(typeof(IEnumerable<object>)))
        {
            return (TResult)(object)limited;
        }

        throw new NotSupportedException($"Result type '{typeof(TResult)}' not supported for LimitPlan");
    }

    private TResult ExecuteOffset<TResult>(OffsetPlan offset)
    {
        // Execute input first
        var inputResult = Execute<IEnumerable<object>>(offset.Input);
        
        // Apply offset
        var skipped = inputResult.Skip(offset.Count);
        
        // Convert to expected result type
        if (typeof(TResult).IsAssignableFrom(typeof(IEnumerable<object>)))
        {
            return (TResult)(object)skipped;
        }

        throw new NotSupportedException($"Result type '{typeof(TResult)}' not supported for OffsetPlan");
    }

    private TResult ExecuteProject<TResult>(ProjectPlan project)
    {
        // Execute input to get all rows
        var inputResult = Execute<IEnumerable<object>>(project.Input);
        
        // If no specific projections or projecting all columns, just pass through
        if (project.Projections.Count == 0)
        {
            return (TResult)(object)inputResult;
        }
        
        // Apply projection to each row
        var projectedResults = new List<object>();
        
        foreach (var item in inputResult)
        {
            var projected = ProjectRow(item, project.Projections);
            projectedResults.Add(projected);
        }
        
        // Return based on result type
        var resultType = typeof(TResult);
        
        if (resultType == typeof(int))
        {
            return (TResult)(object)projectedResults.Count;
        }
        
        if (resultType == typeof(long))
        {
            return (TResult)(object)(long)projectedResults.Count;
        }
        
        if (resultType.IsGenericType)
        {
            var genericType = resultType.GetGenericTypeDefinition();
            if (genericType == typeof(IEnumerable<>) || genericType == typeof(IQueryable<>))
            {
                return (TResult)(object)projectedResults;
            }
        }

        // Single element result
        if (projectedResults.Count > 0)
        {
            return (TResult)projectedResults[0];
        }

        throw new InvalidOperationException("Sequence contains no elements.");
    }

    /// <summary>
    /// Projects a single row to include only selected columns.
    /// Phase B: Column projection support.
    /// </summary>
    private static object ProjectRow(object sourceRow, IReadOnlyList<ProjectionColumn> projections)
    {
        // For simple pass-through projections, we need to create a new object
        // with only the selected properties
        
        // If all columns are projected in the same order, return as-is
        var sourceType = sourceRow.GetType();
        var sourceProps = sourceType.GetProperties();
        
        // Check if this is a subset projection
        if (projections.Count == sourceProps.Length)
        {
            // Check if all properties match
            bool allMatch = true;
            for (int i = 0; i < projections.Count; i++)
            {
                if (projections[i].Kind != ProjectionKind.Column ||
                    projections[i].SourceColumn != sourceProps[i].Name)
                {
                    allMatch = false;
                    break;
                }
            }
            
            if (allMatch)
            {
                return sourceRow; // No projection needed
            }
        }
        
        // Need to create projected object
        // For now, create an anonymous type-like object using a dictionary
        // This is a simplified implementation
        
        // In a full implementation, you'd use:
        // 1. Runtime code generation (System.Reflection.Emit)
        // 2. Anonymous types
        // 3. Dynamic objects
        
        // For Phase B, we'll create a ProjectedRow wrapper
        var projectedValues = new Dictionary<string, object?>();
        
        foreach (var proj in projections)
        {
            if (proj.Kind == ProjectionKind.Column && proj.SourceColumn != null)
            {
                var sourceProp = sourceType.GetProperty(proj.SourceColumn);
                if (sourceProp != null)
                {
                    var value = sourceProp.GetValue(sourceRow);
                    projectedValues[proj.OutputName] = value;
                }
            }
        }
        
        return new ProjectedRow(projectedValues);
    }

    /// <summary>
    /// Represents a projected row with selected columns.
    /// </summary>
    private sealed class ProjectedRow(Dictionary<string, object?> values)
    {
        private readonly Dictionary<string, object?> _values = values;

        public object? GetValue(string columnName)
        {
            return _values.TryGetValue(columnName, out var value) ? value : null;
        }

        public override string ToString()
        {
            return $"{{ {string.Join(", ", _values.Select(kv => $"{kv.Key}={kv.Value}"))} }}";
        }

        public override bool Equals(object? obj)
        {
            if (obj is ProjectedRow other)
            {
                if (_values.Count != other._values.Count) return false;
                
                foreach (var kv in _values)
                {
                    if (!other._values.TryGetValue(kv.Key, out var otherValue))
                        return false;
                    
                    if (!Equals(kv.Value, otherValue))
                        return false;
                }
                
                return true;
            }
            
            return false;
        }

        public override int GetHashCode()
        {
            var hash = new HashCode();
            foreach (var kv in _values.OrderBy(kv => kv.Key))
            {
                hash.Add(kv.Key);
                hash.Add(kv.Value);
            }
            return hash.ToHashCode();
        }
    }

    /// <summary>
    /// Executes DISTINCT operation - removes duplicate rows.
    /// Phase B: DISTINCT support.
    /// </summary>
    private TResult ExecuteDistinct<TResult>(DistinctPlan distinct)
    {
        // Execute the input plan to get all rows
        var inputResult = Execute<IEnumerable<object>>(distinct.Input);
        
        // Deduplicate using HashSet
        // This preserves insertion order in .NET (since .NET Core 3.0)
        var seen = new HashSet<object>(new ObjectEqualityComparer());
        var distinctResults = new List<object>();
        
        foreach (var item in inputResult)
        {
            if (seen.Add(item))
            {
                distinctResults.Add(item);
            }
        }

        // Return based on result type
        var resultType = typeof(TResult);
        
        if (resultType == typeof(int))
        {
            return (TResult)(object)distinctResults.Count;
        }
        
        if (resultType == typeof(long))
        {
            return (TResult)(object)(long)distinctResults.Count;
        }
        
        if (resultType.IsGenericType)
        {
            var genericType = resultType.GetGenericTypeDefinition();
            if (genericType == typeof(IEnumerable<>) || genericType == typeof(IQueryable<>))
            {
                // Return the distinct list as IEnumerable<T>
                return (TResult)(object)distinctResults;
            }
        }

        // Single element result
        if (distinctResults.Count > 0)
        {
            return (TResult)distinctResults[0];
        }

        throw new InvalidOperationException("Sequence contains no elements.");
    }

    /// <summary>
    /// Executes ORDER BY operation - sorts rows by specified columns.
    /// Phase B: ORDER BY support.
    /// </summary>
    private TResult ExecuteSort<TResult>(SortPlan sort)
    {
        // Execute the input plan to get all rows
        var inputResult = Execute<IEnumerable<object>>(sort.Input);
        
        // Convert to list for sorting
        var items = inputResult.ToList();
        
        // Build a comparison function based on sort specifications
        var comparer = new MultiColumnComparer(sort.SortSpecifications, _columnIndexMap, _recordBatch);
        
        // Sort the items
        items.Sort(comparer);
        
        // Return based on result type
        var resultType = typeof(TResult);
        
        if (resultType == typeof(int))
        {
            return (TResult)(object)items.Count;
        }
        
        if (resultType == typeof(long))
        {
            return (TResult)(object)(long)items.Count;
        }
        
        if (resultType.IsGenericType)
        {
            var genericType = resultType.GetGenericTypeDefinition();
            if (genericType == typeof(IEnumerable<>) || genericType == typeof(IQueryable<>))
            {
                // Return the sorted list as IEnumerable<T>
                return (TResult)(object)items;
            }
        }

        // Single element result
        if (items.Count > 0)
        {
            return (TResult)items[0];
        }

        throw new InvalidOperationException("Sequence contains no elements.");
    }

    /// <summary>
    /// Comparer that sorts by multiple columns with different directions.
    /// </summary>
    private sealed class MultiColumnComparer : IComparer<object>
    {
        private readonly IReadOnlyList<SortSpecification> _sortSpecs;
        private readonly Dictionary<string, int> _columnIndexMap;
        private readonly RecordBatch _recordBatch;

        public MultiColumnComparer(
            IReadOnlyList<SortSpecification> sortSpecs,
            Dictionary<string, int> columnIndexMap,
            RecordBatch recordBatch)
        {
            _sortSpecs = sortSpecs;
            _columnIndexMap = columnIndexMap;
            _recordBatch = recordBatch;
        }

        public int Compare(object? x, object? y)
        {
            if (x == null && y == null) return 0;
            if (x == null) return -1;
            if (y == null) return 1;

            // For each sort specification, compare the column values
            foreach (var sortSpec in _sortSpecs)
            {
                var xValue = GetPropertyValue(x, sortSpec.ColumnName);
                var yValue = GetPropertyValue(y, sortSpec.ColumnName);
                
                int comparison;
                
                if (xValue == null && yValue == null)
                {
                    comparison = 0;
                }
                else if (xValue == null)
                {
                    comparison = -1;
                }
                else if (yValue == null)
                {
                    comparison = 1;
                }
                else if (xValue is IComparable comparableX)
                {
                    comparison = comparableX.CompareTo(yValue);
                }
                else
                {
                    // Fallback to string comparison
                    comparison = string.Compare(xValue?.ToString(), yValue?.ToString(), StringComparison.Ordinal);
                }
                
                // If not equal, return based on sort direction
                if (comparison != 0)
                {
                    return sortSpec.Direction == SortDirection.Ascending ? comparison : -comparison;
                }
                
                // If equal, continue to next sort column
            }
            
            return 0;
        }

        private static object? GetPropertyValue(object obj, string propertyName)
        {
            // Use reflection to get property value
            var prop = obj.GetType().GetProperty(propertyName);
            if (prop != null)
            {
                return prop.GetValue(obj);
            }
            
            return null;
        }
    }

    /// <summary>
    /// Equality comparer for DISTINCT that compares objects by their property values.
    /// </summary>
    private sealed class ObjectEqualityComparer : IEqualityComparer<object>
    {
        public new bool Equals(object? x, object? y)
        {
            if (ReferenceEquals(x, y)) return true;
            if (x == null || y == null) return false;
            
            // For records, default Equals should work (value-based equality)
            return x.Equals(y);
        }

        public int GetHashCode(object obj)
        {
            return obj?.GetHashCode() ?? 0;
        }
    }

    private object CreateBatchedEnumerable(List<int> selectedIndices)
    {
        // Create a generic BatchedEnumerator<T> using reflection
        // This ensures proper type compatibility with IEnumerable<T>
        var elementType = _createItem(_recordBatch, 0).GetType();
        var enumeratorType = typeof(BatchedEnumerator<>).MakeGenericType(elementType);
        var constructor = enumeratorType.GetConstructor([typeof(RecordBatch), typeof(List<int>), typeof(Func<RecordBatch, int, object>)])!;
        return constructor.Invoke([_recordBatch, selectedIndices, _createItem]);
    }

    private static Type? GetElementType(Type type)
    {
        // Check if this is the element type itself
        if (!type.IsGenericType)
        {
            return type;
        }

        return null;
    }

    /// <summary>
    /// Batched enumerator for efficient enumeration with proper generic type.
    /// </summary>
    private sealed class BatchedEnumerator<T>(RecordBatch batch, List<int> indices, Func<RecordBatch, int, object> createItem) 
        : IEnumerable<T>, IEnumerator<T>
    {
        private readonly Func<RecordBatch, int, object> _createItem = createItem;
        private int _position = -1;

        public T Current => (T)_createItem(batch, indices[_position]);

        object System.Collections.IEnumerator.Current => Current!;

        public bool MoveNext()
        {
            _position++;
            return _position < indices.Count;
        }

        public void Reset() => _position = -1;

        public IEnumerator<T> GetEnumerator() => this;

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => this;

        public void Dispose() { }
    }
}
