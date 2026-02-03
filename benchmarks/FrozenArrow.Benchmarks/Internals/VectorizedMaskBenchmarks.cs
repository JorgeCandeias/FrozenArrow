using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using FrozenArrow.Query;

namespace FrozenArrow.Benchmarks.Internals;

/// <summary>
/// Benchmarks for SIMD predicate evaluation with vectorized mask application.
/// </summary>
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[GroupBenchmarksBy(BenchmarkDotNet.Configs.BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
[ShortRunJob]
public class VectorizedMaskBenchmarks
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

    #region Int32 SIMD Comparison

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Int32Comparison")]
    public int Int32_List_Where()
    {
        return _list.Count(x => x.Age > 30);
    }

    [Benchmark]
    [BenchmarkCategory("Int32Comparison")]
    public int Int32_FrozenArrow_Where()
    {
        return _frozenArrow.AsQueryable().Where(x => x.Age > 30).Count();
    }

    #endregion

    #region Double SIMD Comparison

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("DoubleComparison")]
    public int Double_List_Where()
    {
        return _list.Count(x => x.PerformanceScore > 75.0);
    }

    [Benchmark]
    [BenchmarkCategory("DoubleComparison")]
    public int Double_FrozenArrow_Where()
    {
        return _frozenArrow.AsQueryable().Where(x => x.PerformanceScore > 75.0).Count();
    }

    #endregion

    #region Multiple Int32 Comparisons (exercises SIMD mask application heavily)

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("MultipleInt32")]
    public int MultiInt32_List_Where()
    {
        return _list.Count(x => x.Age > 25 && x.Age < 50);
    }

    [Benchmark]
    [BenchmarkCategory("MultipleInt32")]
    public int MultiInt32_FrozenArrow_Where()
    {
        return _frozenArrow.AsQueryable().Where(x => x.Age > 25 && x.Age < 50).Count();
    }

    #endregion

    #region Complex Predicate (Int + Double + Bool)

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("ComplexPredicate")]
    public int Complex_List_Where()
    {
        return _list.Count(x => x.Age > 30 && x.PerformanceScore > 50.0 && x.IsActive);
    }

    [Benchmark]
    [BenchmarkCategory("ComplexPredicate")]
    public int Complex_FrozenArrow_Where()
    {
        return _frozenArrow.AsQueryable()
            .Where(x => x.Age > 30 && x.PerformanceScore > 50.0 && x.IsActive)
            .Count();
    }

    #endregion
}
