namespace FrozenArrow.Query;

/// <summary>
/// Extension methods for creating ArrowQuery instances.
/// </summary>
public static class ArrowQueryExtensions
{
    /// <summary>
    /// Creates an IQueryable{T} over the FrozenArrow collection that enables optimized LINQ queries.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ArrowQuery provides optimized execution of LINQ queries by pushing filter operations
    /// down to the Arrow column level. Instead of materializing all objects and then filtering,
    /// ArrowQuery evaluates predicates directly against the columnar data.
    /// </para>
    /// <para>
    /// Supported LINQ operations:
    /// <list type="bullet">
    ///   <item>Where - Filter predicates are pushed to column-level evaluation</item>
    ///   <item>Select - Column projection (accesses only required columns)</item>
    ///   <item>First, FirstOrDefault, Single, SingleOrDefault - Single element access</item>
    ///   <item>Any, All - Boolean predicates</item>
    ///   <item>Count, LongCount - Counting with optional predicate</item>
    ///   <item>Take, Skip - Pagination</item>
    ///   <item>OrderBy, OrderByDescending, ThenBy, ThenByDescending - Sorting</item>
    ///   <item>ToList, ToArray - Materialization</item>
    /// </list>
    /// </para>
    /// <para>
    /// Example:
    /// <code>
    /// var results = collection
    ///     .AsQueryable()
    ///     .Where(x => x.Age > 30 &amp;&amp; x.Category == "Premium")
    ///     .OrderBy(x => x.Name)
    ///     .Take(100)
    ///     .ToList();
    /// </code>
    /// </para>
    /// </remarks>
    /// <typeparam name="T">The element type of the collection.</typeparam>
    /// <param name="collection">The FrozenArrow collection to query.</param>
    /// <returns>An IQueryable{T} that executes queries against the Arrow columnar data.</returns>
    public static IQueryable<T> AsQueryable<T>(this FrozenArrow<T> collection)
    {
        ArgumentNullException.ThrowIfNull(collection);
        return new ArrowQuery<T>(collection);
    }

    /// <summary>
    /// Creates an ArrowQuery over the FrozenArrow collection with explicit access to query features.
    /// </summary>
    /// <remarks>
    /// This method returns the concrete ArrowQuery{T} type, which provides additional
    /// methods like Explain() for debugging query plans.
    /// </remarks>
    /// <typeparam name="T">The element type of the collection.</typeparam>
    /// <param name="collection">The FrozenArrow collection to query.</param>
    /// <returns>An ArrowQuery{T} instance.</returns>
    public static ArrowQuery<T> Query<T>(this FrozenArrow<T> collection)
    {
        ArgumentNullException.ThrowIfNull(collection);
        return new ArrowQuery<T>(collection);
    }

    /// <summary>
    /// Returns a string explaining the query execution plan.
    /// </summary>
    /// <remarks>
    /// This is useful for debugging and understanding how the query will be executed.
    /// The output includes:
    /// <list type="bullet">
    ///   <item>Whether the query is fully optimized</item>
    ///   <item>Which columns will be accessed</item>
    ///   <item>What predicates will be pushed to column-level</item>
    ///   <item>Any unsupported operations that will cause fallback</item>
    /// </list>
    /// </remarks>
    /// <typeparam name="T">The element type of the query.</typeparam>
    /// <param name="query">The query to explain.</param>
    /// <returns>A string describing the query plan.</returns>
    public static string Explain<T>(this IQueryable<T> query)
    {
        if (query is ArrowQuery<T> arrowQuery)
        {
            return arrowQuery.Explain();
        }

        return "Query is not an ArrowQuery - cannot provide execution plan.";
    }

    /// <summary>
    /// Configures the query to allow fallback to full materialization for unsupported operations.
    /// </summary>
    /// <remarks>
    /// By default, ArrowQuery operates in strict mode and throws NotSupportedException
    /// for operations that cannot be optimized. Calling AllowFallback() disables strict mode,
    /// allowing the query to fall back to standard LINQ-to-Objects for unsupported operations.
    /// 
    /// Warning: This can result in poor performance if the fallback causes full materialization
    /// of large datasets.
    /// </remarks>
    /// <typeparam name="T">The element type of the query.</typeparam>
    /// <param name="query">The query to configure.</param>
    /// <returns>The same query with fallback enabled.</returns>
    public static IQueryable<T> AllowFallback<T>(this IQueryable<T> query)
    {
        if (query is ArrowQuery<T> arrowQuery)
        {
            // Access the provider and disable strict mode
            if (arrowQuery.Provider is ArrowQueryProvider provider)
            {
                provider.StrictMode = false;
            }
        }

        return query;
    }

    /// <summary>
    /// Configures the query to throw on unsupported operations (default behavior).
    /// </summary>
    /// <typeparam name="T">The element type of the query.</typeparam>
    /// <param name="query">The query to configure.</param>
    /// <returns>The same query with strict mode enabled.</returns>
    public static IQueryable<T> StrictMode<T>(this IQueryable<T> query)
    {
        if (query is ArrowQuery<T> arrowQuery)
        {
            if (arrowQuery.Provider is ArrowQueryProvider provider)
            {
                provider.StrictMode = true;
            }
        }

        return query;
    }

    /// <summary>
    /// Configures parallel execution options for the query.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Parallel execution can significantly improve query performance on large datasets
    /// by evaluating predicates across multiple CPU cores simultaneously. The data is
    /// partitioned into chunks, and each chunk is processed independently.
    /// </para>
    /// <para>
    /// Example:
    /// <code>
    /// var results = collection
    ///     .AsQueryable()
    ///     .WithParallelOptions(new ParallelQueryOptions
    ///     {
    ///         ChunkSize = 32_768,           // 32K rows per chunk
    ///         MaxDegreeOfParallelism = 4    // Use up to 4 threads
    ///     })
    ///     .Where(x => x.Age > 30 &amp;&amp; x.Category == "Premium")
    ///     .ToList();
    /// </code>
    /// </para>
    /// </remarks>
    /// <typeparam name="T">The element type of the query.</typeparam>
    /// <param name="query">The query to configure.</param>
    /// <param name="options">The parallel execution options.</param>
    /// <returns>The same query with the specified parallel options.</returns>
    public static IQueryable<T> WithParallelOptions<T>(this IQueryable<T> query, ParallelQueryOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (query is ArrowQuery<T> arrowQuery)
        {
            if (arrowQuery.Provider is ArrowQueryProvider provider)
            {
                provider.ParallelOptions = options;
            }
        }

        return query;
    }

    /// <summary>
    /// Enables parallel execution for the query with default options.
    /// </summary>
    /// <typeparam name="T">The element type of the query.</typeparam>
    /// <param name="query">The query to configure.</param>
    /// <returns>The same query with parallel execution enabled.</returns>
    public static IQueryable<T> AsParallel<T>(this IQueryable<T> query)
    {
        return query.WithParallelOptions(ParallelQueryOptions.Default);
    }

    /// <summary>
    /// Disables parallel execution for the query, forcing sequential evaluation.
    /// </summary>
    /// <remarks>
    /// This can be useful for debugging or when parallel overhead is not beneficial
    /// for smaller datasets.
    /// </remarks>
    /// <typeparam name="T">The element type of the query.</typeparam>
    /// <param name="query">The query to configure.</param>
    /// <returns>The same query with parallel execution disabled.</returns>
    public static IQueryable<T> AsSequential<T>(this IQueryable<T> query)
    {
        return query.WithParallelOptions(new ParallelQueryOptions { EnableParallelExecution = false });
    }

    /// <summary>
    /// Computes multiple aggregates over the query results in a single pass.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This method allows computing multiple aggregate values (Sum, Average, Min, Max, Count)
    /// in a single pass over the data, which is more efficient than calling each aggregate
    /// method separately.
    /// </para>
    /// <para>
    /// Example:
    /// <code>
    /// var stats = collection
    ///     .AsQueryable()
    ///     .Where(x => x.IsActive)
    ///     .Aggregate(agg => new SalaryStats
    ///     {
    ///         TotalSalary = agg.Sum(x => x.Salary),
    ///         AverageAge = agg.Average(x => x.Age),
    ///         MinSalary = agg.Min(x => x.Salary),
    ///         MaxSalary = agg.Max(x => x.Salary),
    ///         Count = agg.Count()
    ///     });
    /// </code>
    /// </para>
    /// </remarks>
    /// <typeparam name="T">The element type of the query.</typeparam>
    /// <typeparam name="TResult">The result type containing the aggregate values.</typeparam>
    /// <param name="query">The query to aggregate.</param>
    /// <param name="aggregateSelector">
    /// A function that uses the AggregateBuilder to define the aggregates and maps them to a result type.
    /// </param>
    /// <returns>An instance of TResult containing all the computed aggregate values.</returns>
    public static TResult Aggregate<T, TResult>(
        this IQueryable<T> query,
        System.Linq.Expressions.Expression<Func<AggregateBuilder<T>, TResult>> aggregateSelector)
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(aggregateSelector);

        if (query is not ArrowQuery<T> arrowQuery)
        {
            throw new NotSupportedException("Aggregate is only supported on ArrowQuery<T>.");
        }

        if (arrowQuery.Provider is not ArrowQueryProvider provider)
        {
            throw new NotSupportedException("Aggregate requires ArrowQueryProvider.");
        }

        return provider.ExecuteMultiAggregate(arrowQuery.Expression, aggregateSelector);
    }

    /// <summary>
    /// Creates a Dictionary from a grouped query, using the group key as the dictionary key
    /// and computing an aggregate as the dictionary value.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This method provides optimized execution for the common pattern of:
    /// <code>
    /// collection.AsQueryable()
    ///     .GroupBy(x => x.Category)
    ///     .ToDictionary(g => g.Key, g => g.Count());
    /// </code>
    /// </para>
    /// <para>
    /// The aggregation is computed at the column level without materializing the grouped items.
    /// Supported aggregates: Count(), LongCount(), Sum(x => x.Col), Average(x => x.Col), 
    /// Min(x => x.Col), Max(x => x.Col).
    /// </para>
    /// </remarks>
    /// <typeparam name="TKey">The type of the grouping key.</typeparam>
    /// <typeparam name="TSource">The type of the source elements.</typeparam>
    /// <typeparam name="TValue">The type of the dictionary values (aggregate result).</typeparam>
    /// <param name="source">The grouped query.</param>
    /// <param name="keySelector">A function to extract the key (must be g => g.Key).</param>
    /// <param name="elementSelector">A function to compute the value (must be an aggregate like g.Count()).</param>
    /// <returns>A Dictionary with group keys and aggregated values.</returns>
    public static Dictionary<TKey, TValue> ToDictionary<TKey, TSource, TValue>(
        this IQueryable<IGrouping<TKey, TSource>> source,
        System.Linq.Expressions.Expression<Func<IGrouping<TKey, TSource>, TKey>> keySelector,
        System.Linq.Expressions.Expression<Func<IGrouping<TKey, TSource>, TValue>> elementSelector)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(keySelector);
        ArgumentNullException.ThrowIfNull(elementSelector);

        // Check if this is an ArrowQuery - if so, use optimized path
        if (source.Provider is ArrowQueryProvider provider)
        {
            // Build a method call expression for ToDictionary
            var toDictionaryCall = System.Linq.Expressions.Expression.Call(
                typeof(ArrowQueryExtensions),
                nameof(ToDictionary),
                [typeof(TKey), typeof(TSource), typeof(TValue)],
                source.Expression,
                keySelector,
                elementSelector);

            // Execute through the provider
            return provider.Execute<Dictionary<TKey, TValue>>(toDictionaryCall);
        }

        // Fallback to standard LINQ
        return Enumerable.ToDictionary(source, keySelector.Compile(), elementSelector.Compile());
    }
}
