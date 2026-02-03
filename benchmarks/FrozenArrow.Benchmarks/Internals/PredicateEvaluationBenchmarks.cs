using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using FrozenArrow.Query;

namespace FrozenArrow.Benchmarks.Internals;

/// <summary>
/// Benchmarks for predicate evaluation on Arrow columns.
/// These are internal component benchmarks for optimization work.
/// </summary>
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[GroupBenchmarksBy(BenchmarkDotNet.Configs.BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
[ShortRunJob]
public class PredicateEvaluationBenchmarks
{
    private List<QueryBenchmarkItem> _list = null!;
    private FrozenArrow<QueryBenchmarkItem> _frozenArrow = null!;

    [Params(10_000, 100_000, 1_000_000)]
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

    #region Int32 Predicate - High Selectivity (~5%)

    [Benchmark]
    [BenchmarkCategory("Int32_HighSelectivity")]
    public int List_Int32_HighSelectivity()
    {
        return _list.Where(x => x.Age > 55).Count();
    }

    [Benchmark]
    [BenchmarkCategory("Int32_HighSelectivity")]
    public int FrozenArrow_Int32_HighSelectivity()
    {
        return _frozenArrow.AsQueryable().Where(x => x.Age > 55).Count();
    }

    #endregion

    #region Int32 Predicate - Medium Selectivity (~50%)

    [Benchmark]
    [BenchmarkCategory("Int32_MediumSelectivity")]
    public int List_Int32_MediumSelectivity()
    {
        return _list.Where(x => x.Age > 30).Count();
    }

    [Benchmark]
    [BenchmarkCategory("Int32_MediumSelectivity")]
    public int FrozenArrow_Int32_MediumSelectivity()
    {
        return _frozenArrow.AsQueryable().Where(x => x.Age > 30).Count();
    }

    #endregion

    #region Int32 Predicate - Low Selectivity (~95%)

    [Benchmark]
    [BenchmarkCategory("Int32_LowSelectivity")]
    public int List_Int32_LowSelectivity()
    {
        return _list.Where(x => x.Age > 10).Count();
    }

    [Benchmark]
    [BenchmarkCategory("Int32_LowSelectivity")]
    public int FrozenArrow_Int32_LowSelectivity()
    {
        return _frozenArrow.AsQueryable().Where(x => x.Age > 10).Count();
    }

    #endregion

    #region Double Predicate - High Selectivity (~5%)

    [Benchmark]
    [BenchmarkCategory("Double_HighSelectivity")]
    public int List_Double_HighSelectivity()
    {
        return _list.Where(x => x.PerformanceScore > 4.5).Count();
    }

    [Benchmark]
    [BenchmarkCategory("Double_HighSelectivity")]
    public int FrozenArrow_Double_HighSelectivity()
    {
        return _frozenArrow.AsQueryable().Where(x => x.PerformanceScore > 4.5).Count();
    }

    #endregion

    #region Double Predicate - Medium Selectivity (~50%)

    [Benchmark]
    [BenchmarkCategory("Double_MediumSelectivity")]
    public int List_Double_MediumSelectivity()
    {
        return _list.Where(x => x.PerformanceScore > 2.5).Count();
    }

    [Benchmark]
    [BenchmarkCategory("Double_MediumSelectivity")]
    public int FrozenArrow_Double_MediumSelectivity()
    {
        return _frozenArrow.AsQueryable().Where(x => x.PerformanceScore > 2.5).Count();
    }

    #endregion

    #region Multiple Predicates (Tests predicate combination efficiency)

    [Benchmark]
    [BenchmarkCategory("MultiPredicate")]
    public int List_MultiPredicate()
    {
        return _list
            .Where(x => x.Age > 25)
            .Where(x => x.Age < 55)
            .Where(x => x.IsActive)
            .Count();
    }

    [Benchmark]
    [BenchmarkCategory("MultiPredicate")]
    public int FrozenArrow_MultiPredicate()
    {
        return _frozenArrow.AsQueryable()
            .Where(x => x.Age > 25)
            .Where(x => x.Age < 55)
            .Where(x => x.IsActive)
            .Count();
    }

    #endregion
}
