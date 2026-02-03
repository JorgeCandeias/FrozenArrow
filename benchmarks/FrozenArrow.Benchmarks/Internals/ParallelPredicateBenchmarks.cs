using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using FrozenArrow.Query;

namespace FrozenArrow.Benchmarks.Internals;

/// <summary>
/// Benchmarks comparing sequential vs parallel predicate evaluation.
/// </summary>
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[GroupBenchmarksBy(BenchmarkDotNet.Configs.BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
[ShortRunJob]
public class ParallelPredicateBenchmarks
{
    private List<QueryBenchmarkItem> _list = null!;
    private FrozenArrow<QueryBenchmarkItem> _frozenArrow = null!;

    [Params(100_000, 1_000_000)]
    public int ItemCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _list = QueryBenchmarkItemFactory.Generate(ItemCount);
        _frozenArrow = _list.ToFrozenArrow();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _frozenArrow.Dispose();
    }

    #region Single Predicate - Sequential vs Parallel

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("SinglePredicate")]
    public int Sequential_SinglePredicate()
    {
        var query = _frozenArrow.AsQueryable();
        ((ArrowQueryProvider)query.Provider).ParallelOptions = new ParallelQueryOptions { EnableParallelExecution = false };
        return query.Where(x => x.Age > 30).Count();
    }

    [Benchmark]
    [BenchmarkCategory("SinglePredicate")]
    public int Parallel_SinglePredicate()
    {
        var query = _frozenArrow.AsQueryable();
        ((ArrowQueryProvider)query.Provider).ParallelOptions = new ParallelQueryOptions { EnableParallelExecution = true };
        return query.Where(x => x.Age > 30).Count();
    }

    #endregion

    #region Multiple Predicates - Sequential vs Parallel

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("MultiplePredicates")]
    public int Sequential_MultiplePredicates()
    {
        var query = _frozenArrow.AsQueryable();
        ((ArrowQueryProvider)query.Provider).ParallelOptions = new ParallelQueryOptions { EnableParallelExecution = false };
        return query.Where(x => x.Age > 25 && x.Age < 50 && x.IsActive).Count();
    }

    [Benchmark]
    [BenchmarkCategory("MultiplePredicates")]
    public int Parallel_MultiplePredicates()
    {
        var query = _frozenArrow.AsQueryable();
        ((ArrowQueryProvider)query.Provider).ParallelOptions = new ParallelQueryOptions { EnableParallelExecution = true };
        return query.Where(x => x.Age > 25 && x.Age < 50 && x.IsActive).Count();
    }

    #endregion

    #region With Aggregation - Sequential vs Parallel

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("WithAggregation")]
    public decimal Sequential_WithAggregation()
    {
        var query = _frozenArrow.AsQueryable();
        ((ArrowQueryProvider)query.Provider).ParallelOptions = new ParallelQueryOptions { EnableParallelExecution = false };
        return query.Where(x => x.Age > 30 && x.IsActive).Sum(x => x.Salary);
    }

    [Benchmark]
    [BenchmarkCategory("WithAggregation")]
    public decimal Parallel_WithAggregation()
    {
        var query = _frozenArrow.AsQueryable();
        ((ArrowQueryProvider)query.Provider).ParallelOptions = new ParallelQueryOptions { EnableParallelExecution = true };
        return query.Where(x => x.Age > 30 && x.IsActive).Sum(x => x.Salary);
    }

    #endregion

    #region Chunk Size Tuning

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("ChunkSize")]
    public int ChunkSize_8K()
    {
        var query = _frozenArrow.AsQueryable();
        ((ArrowQueryProvider)query.Provider).ParallelOptions = new ParallelQueryOptions 
        { 
            EnableParallelExecution = true,
            ChunkSize = 8_192
        };
        return query.Where(x => x.Age > 30).Count();
    }

    [Benchmark]
    [BenchmarkCategory("ChunkSize")]
    public int ChunkSize_16K()
    {
        var query = _frozenArrow.AsQueryable();
        ((ArrowQueryProvider)query.Provider).ParallelOptions = new ParallelQueryOptions 
        { 
            EnableParallelExecution = true,
            ChunkSize = 16_384
        };
        return query.Where(x => x.Age > 30).Count();
    }

    [Benchmark]
    [BenchmarkCategory("ChunkSize")]
    public int ChunkSize_32K()
    {
        var query = _frozenArrow.AsQueryable();
        ((ArrowQueryProvider)query.Provider).ParallelOptions = new ParallelQueryOptions 
        { 
            EnableParallelExecution = true,
            ChunkSize = 32_768
        };
        return query.Where(x => x.Age > 30).Count();
    }

    [Benchmark]
    [BenchmarkCategory("ChunkSize")]
    public int ChunkSize_64K()
    {
        var query = _frozenArrow.AsQueryable();
        ((ArrowQueryProvider)query.Provider).ParallelOptions = new ParallelQueryOptions 
        { 
            EnableParallelExecution = true,
            ChunkSize = 65_536
        };
        return query.Where(x => x.Age > 30).Count();
    }

    #endregion
}
