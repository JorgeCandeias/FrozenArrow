using System.Diagnostics;
using FrozenArrow.Query;

namespace FrozenArrow.Profiling.Scenarios;

/// <summary>
/// Profiles query plan caching performance by measuring repeated query execution.
/// </summary>
/// <remarks>
/// Query plan caching eliminates the overhead of expression tree analysis (~2-3ms per query).
/// This scenario measures:
/// - Cold query (cache miss): Full expression analysis
/// - Warm queries (cache hit): Cached plan reuse
/// - Cache hit rate and statistics
/// 
/// The improvement is most significant for:
/// - Short-circuit operations (Any, First) where parsing dominates
/// - Repeated queries with the same structure
/// - High-frequency query workloads
/// 
/// Expected improvements:
/// - Cache hit queries: 50-90% faster than cold queries
/// - Short-circuit with early match: 2-5x overall speedup
/// </remarks>
public sealed class QueryPlanCacheScenario : BaseScenario
{
    public override string Name => "QueryPlanCache";
    public override string Description => "Query plan caching performance for repeated queries";

    // Track statistics
    private long _cacheHits;
    private long _cacheMisses;
    private double _coldQueryTimeUs;
    private double _warmQueryTimeUs;

    public override object? RunIteration()
    {
        // Clear cache to get consistent results
        var query = Data.AsQueryable();
        var provider = (ArrowQueryProvider)query.Provider;
        provider.ClearQueryPlanCache();

        // Cold query (cache miss)
        var result1 = Data.AsQueryable().Where(x => x.Age > 30).Any();
        
        // Warm queries (cache hit) - run multiple times
        var result2 = false;
        for (int i = 0; i < 10; i++)
        {
            result2 = Data.AsQueryable().Where(x => x.Age > 30).Any();
        }

        return result1 && result2;
    }

    public override (object? Result, Dictionary<string, double> PhaseMicroseconds) RunIterationWithPhases()
    {
        CurrentPhases.Clear();
        var sw = Stopwatch.StartNew();

        // Clear cache for consistent baseline (cache is now at FrozenArrow level)
        Data.ClearQueryPlanCache();

        // Phase 1: Cold query (cache miss) - First execution of a query pattern
        // The expression tree is analyzed and the plan is cached
        // NOTE: Each AsQueryable() call creates a new provider, but they all share
        // the same cache from the FrozenArrow instance
        StartPhase("ColdQuery_Any");
        var coldResult = Data.AsQueryable().Where(x => x.Age > 30).Any();
        EndPhase("ColdQuery_Any");
        _coldQueryTimeUs = CurrentPhases["ColdQuery_Any"];

        // Phase 2: Warm query (cache hit) - Same query structure, plan retrieved from cache
        // Each iteration uses a NEW AsQueryable() call - cache is still shared!
        StartPhase("WarmQuery_Any_x10");
        for (int i = 0; i < 10; i++)
        {
            // Each iteration uses a NEW AsQueryable() - cache is still shared at FrozenArrow level!
            _ = Data.AsQueryable().Where(x => x.Age > 30).Any();
        }
        EndPhase("WarmQuery_Any_x10");
        _warmQueryTimeUs = CurrentPhases["WarmQuery_Any_x10"] / 10.0; // Average per query

        // Phase 3: Different cold query (cache miss) - different predicate pattern
        StartPhase("ColdQuery_First");
        var firstResult = Data.AsQueryable().Where(x => x.Salary > 50000).First();
        EndPhase("ColdQuery_First");

        // Phase 4: Warm query for the same First pattern (cache hit)
        StartPhase("WarmQuery_First_x10");
        for (int i = 0; i < 10; i++)
        {
            _ = Data.AsQueryable().Where(x => x.Salary > 50000).First();
        }
        EndPhase("WarmQuery_First_x10");

        // Phase 5: Cold query with multi-predicate filter (cache miss)
        StartPhase("ColdQuery_Count");
        var count = Data.AsQueryable().Where(x => x.Age > 30 && x.IsActive).Count();
        EndPhase("ColdQuery_Count");

        // Phase 6: Warm query for the same Count pattern (cache hit)
        StartPhase("WarmQuery_Count_x10");
        for (int i = 0; i < 10; i++)
        {
            _ = Data.AsQueryable().Where(x => x.Age > 30 && x.IsActive).Count();
        }
        EndPhase("WarmQuery_Count_x10");

        // Phase 7: Different queries with different constants (each is a separate cache entry)
        // These will all be cache misses since each has a different constant value
        StartPhase("DifferentQueries_x5");
        _ = Data.AsQueryable().Where(x => x.Age > 25).Any();
        _ = Data.AsQueryable().Where(x => x.Age > 26).Any();
        _ = Data.AsQueryable().Where(x => x.Age > 27).Any();
        _ = Data.AsQueryable().Where(x => x.Age > 28).Any();
        _ = Data.AsQueryable().Where(x => x.Age > 29).Any();
        EndPhase("DifferentQueries_x5");

        // Capture cache statistics from the FrozenArrow instance
        _cacheHits = Data.QueryPlanCacheStatistics.Hits;
        _cacheMisses = Data.QueryPlanCacheStatistics.Misses;

        return (
            coldResult && firstResult != null,
            new Dictionary<string, double>(CurrentPhases)
        );
    }

    public override Dictionary<string, string> GetMetadata()
    {
        var speedup = _coldQueryTimeUs > 0 && _warmQueryTimeUs > 0 
            ? _coldQueryTimeUs / _warmQueryTimeUs 
            : 0;
        
        return new Dictionary<string, string>
        {
            ["CacheHits"] = _cacheHits.ToString(),
            ["CacheMisses"] = _cacheMisses.ToString(),
            ["HitRate"] = (_cacheHits + _cacheMisses) > 0 
                ? $"{(double)_cacheHits / (_cacheHits + _cacheMisses):P1}" 
                : "N/A",
            ["ColdQueryTime_us"] = _coldQueryTimeUs.ToString("F1"),
            ["WarmQueryTime_us"] = _warmQueryTimeUs.ToString("F1"),
            ["Speedup"] = speedup.ToString("F1") + "x"
        };
    }
}
