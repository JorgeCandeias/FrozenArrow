using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using FrozenArrow.Query;

namespace FrozenArrow.Benchmarks.Internals;

/// <summary>
/// Benchmarks comparing traditional (bitmap-based) vs fused (single-pass) execution
/// for filtered aggregate queries.
/// </summary>
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[GroupBenchmarksBy(BenchmarkDotNet.Configs.BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
[ShortRunJob]
public class FusedAggregationBenchmarks
{
    private FrozenArrow<FusedBenchmarkItem> _frozenArrow = null!;

    [Params(10_000, 100_000, 1_000_000)]
    public int ItemCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        // Generate data without dictionary encoding so we can use fused execution
        var records = new List<FusedBenchmarkItem>();
        for (int i = 0; i < ItemCount; i++)
        {
            records.Add(new FusedBenchmarkItem
            {
                Id = i,
                Age = 20 + (i % 50),
                Salary = 40000m + (i % 100) * 1000m,
                IsActive = i % 3 != 0
            });
        }

        // Use primitive arrays (not dictionary-encoded) for fused execution compatibility
        _frozenArrow = records.ToFrozenArrow();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _frozenArrow.Dispose();
    }

    #region Sum with Single Filter

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Sum_SingleFilter")]
    public int Traditional_Sum_SingleFilter()
    {
        // Force non-fused execution by using a small threshold
        var query = _frozenArrow.AsQueryable();
        ((ArrowQueryProvider)query.Provider).ParallelOptions = new ParallelQueryOptions 
        { 
            EnableParallelExecution = false 
        };
        return query.Where(x => x.Age > 30).Sum(x => x.Age);
    }

    [Benchmark]
    [BenchmarkCategory("Sum_SingleFilter")]
    public int Fused_Sum_SingleFilter()
    {
        // Fused execution enabled by default for large enough datasets
        var query = _frozenArrow.AsQueryable();
        ((ArrowQueryProvider)query.Provider).ParallelOptions = new ParallelQueryOptions 
        { 
            EnableParallelExecution = false  // Sequential to compare just fused vs bitmap
        };
        return query.Where(x => x.Age > 30).Sum(x => x.Age);
    }

    #endregion

    #region Sum with Multiple Filters

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Sum_MultiFilter")]
    public int Traditional_Sum_MultiFilter()
    {
        var query = _frozenArrow.AsQueryable();
        ((ArrowQueryProvider)query.Provider).ParallelOptions = new ParallelQueryOptions 
        { 
            EnableParallelExecution = false 
        };
        return query.Where(x => x.Age > 25 && x.Age < 50 && x.IsActive).Sum(x => x.Age);
    }

    [Benchmark]
    [BenchmarkCategory("Sum_MultiFilter")]
    public int Fused_Sum_MultiFilter()
    {
        var query = _frozenArrow.AsQueryable();
        ((ArrowQueryProvider)query.Provider).ParallelOptions = new ParallelQueryOptions 
        { 
            EnableParallelExecution = false 
        };
        return query.Where(x => x.Age > 25 && x.Age < 50 && x.IsActive).Sum(x => x.Age);
    }

    #endregion

    #region Average with Filter

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Average_Filter")]
    public double Traditional_Average_Filter()
    {
        var query = _frozenArrow.AsQueryable();
        ((ArrowQueryProvider)query.Provider).ParallelOptions = new ParallelQueryOptions 
        { 
            EnableParallelExecution = false 
        };
        return query.Where(x => x.IsActive).Average(x => x.Age);
    }

    [Benchmark]
    [BenchmarkCategory("Average_Filter")]
    public double Fused_Average_Filter()
    {
        var query = _frozenArrow.AsQueryable();
        ((ArrowQueryProvider)query.Provider).ParallelOptions = new ParallelQueryOptions 
        { 
            EnableParallelExecution = false 
        };
        return query.Where(x => x.IsActive).Average(x => x.Age);
    }

    #endregion

    #region Min/Max with Filter

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("MinMax_Filter")]
    public int Traditional_Min_Filter()
    {
        var query = _frozenArrow.AsQueryable();
        ((ArrowQueryProvider)query.Provider).ParallelOptions = new ParallelQueryOptions 
        { 
            EnableParallelExecution = false 
        };
        return query.Where(x => x.Age > 40).Min(x => x.Age);
    }

    [Benchmark]
    [BenchmarkCategory("MinMax_Filter")]
    public int Fused_Min_Filter()
    {
        var query = _frozenArrow.AsQueryable();
        ((ArrowQueryProvider)query.Provider).ParallelOptions = new ParallelQueryOptions 
        { 
            EnableParallelExecution = false 
        };
        return query.Where(x => x.Age > 40).Min(x => x.Age);
    }

    #endregion

    #region Parallel Fused vs Parallel Traditional

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Parallel_Sum")]
    public int Parallel_Traditional_Sum()
    {
        var query = _frozenArrow.AsQueryable();
        ((ArrowQueryProvider)query.Provider).ParallelOptions = new ParallelQueryOptions 
        { 
            EnableParallelExecution = true,
            ParallelThreshold = int.MaxValue // Force non-fused by making parallel threshold huge
        };
        return query.Where(x => x.Age > 30 && x.IsActive).Sum(x => x.Age);
    }

    [Benchmark]
    [BenchmarkCategory("Parallel_Sum")]
    public int Parallel_Fused_Sum()
    {
        var query = _frozenArrow.AsQueryable();
        ((ArrowQueryProvider)query.Provider).ParallelOptions = new ParallelQueryOptions 
        { 
            EnableParallelExecution = true,
            ParallelThreshold = 10_000 // Low threshold to enable parallel
        };
        return query.Where(x => x.Age > 30 && x.IsActive).Sum(x => x.Age);
    }

    #endregion
}

/// <summary>
/// Test record using primitive types only (no string which gets dictionary-encoded).
/// This allows the fused aggregator to be used.
/// </summary>
[ArrowRecord]
public record FusedBenchmarkItem
{
    [ArrowArray(Name = "Id")]
    public int Id { get; init; }

    [ArrowArray(Name = "Age")]
    public int Age { get; init; }

    [ArrowArray(Name = "Salary")]
    public decimal Salary { get; init; }

    [ArrowArray(Name = "IsActive")]
    public bool IsActive { get; init; }
}
