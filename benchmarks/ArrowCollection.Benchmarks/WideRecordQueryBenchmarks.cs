using ArrowCollection.Query;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;

namespace ArrowCollection.Benchmarks;

/// <summary>
/// Benchmarks for wide records (200 columns) where object reconstruction is expensive.
/// This demonstrates the value of ArrowQuery when filtering wide tables - 
/// avoiding reconstruction of non-matching rows saves significant time.
/// </summary>
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[GroupBenchmarksBy(BenchmarkDotNet.Configs.BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
public class WideRecordQueryBenchmarks
{
    private List<HeavyBenchmarkItem> _list = null!;
    private ArrowCollection<HeavyBenchmarkItem> _arrowCollection = null!;

    /// <summary>
    /// Smaller counts due to wide record size and benchmark time.
    /// </summary>
    [Params(1_000, 10_000)]
    public int ItemCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _list = HeavyBenchmarkItemFactory.Generate(ItemCount);
        _arrowCollection = _list.ToArrowCollection();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _arrowCollection.Dispose();
    }

    #region High Selectivity on Wide Records (~5% match)

    /// <summary>
    /// Baseline: List with LINQ on wide records.
    /// Filter on String01 which has 100 distinct values, so ~1% match.
    /// </summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("WideRecord_HighSelectivity")]
    public int List_WideRecord_HighSelectivity_ToList()
    {
        return _list.Where(x => x.String01 == "Category00_001").ToList().Count;
    }

    /// <summary>
    /// ArrowCollection Enumerable on wide records - reconstructs ALL 200-column objects.
    /// This should be dramatically slower due to reconstruction overhead.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("WideRecord_HighSelectivity")]
    public int ArrowCollection_WideRecord_HighSelectivity_ToList()
    {
        return _arrowCollection.Where(x => x.String01 == "Category00_001").ToList().Count;
    }

    /// <summary>
    /// ArrowQuery on wide records - only reconstructs matching rows.
    /// Should be much faster than Enumerable approach for wide tables.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("WideRecord_HighSelectivity")]
    public int ArrowQuery_WideRecord_HighSelectivity_ToList()
    {
        return _arrowCollection.AsQueryable().Where(x => x.String01 == "Category00_001").ToList().Count;
    }

    #endregion

    #region Count on Wide Records - ArrowQuery should dominate

    /// <summary>
    /// Count with List on wide records.
    /// </summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("WideRecord_Count")]
    public int List_WideRecord_Count()
    {
        return _list.Where(x => x.Int001 > 5000).Count();
    }

    /// <summary>
    /// Count with ArrowCollection Enumerable - reconstructs all items just to count.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("WideRecord_Count")]
    public int ArrowCollection_WideRecord_Count()
    {
        return _arrowCollection.Where(x => x.Int001 > 5000).Count();
    }

    /// <summary>
    /// Count with ArrowQuery - NO reconstruction, just bitmap counting.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("WideRecord_Count")]
    public int ArrowQuery_WideRecord_Count()
    {
        return _arrowCollection.AsQueryable().Where(x => x.Int001 > 5000).Count();
    }

    #endregion

    #region Multi-Column Filter on Wide Records

    /// <summary>
    /// Multi-column filter with List on wide records.
    /// </summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("WideRecord_MultiColumn")]
    public int List_WideRecord_MultiColumn_ToList()
    {
        return _list
            .Where(x => x.String01 == "Category00_050" && x.Double001 > 0.5 && x.Int001 > 1000)
            .ToList().Count;
    }

    [Benchmark]
    [BenchmarkCategory("WideRecord_MultiColumn")]
    public int ArrowCollection_WideRecord_MultiColumn_ToList()
    {
        return _arrowCollection
            .Where(x => x.String01 == "Category00_050" && x.Double001 > 0.5 && x.Int001 > 1000)
            .ToList().Count;
    }

    [Benchmark]
    [BenchmarkCategory("WideRecord_MultiColumn")]
    public int ArrowQuery_WideRecord_MultiColumn_ToList()
    {
        return _arrowCollection
            .AsQueryable()
            .Where(x => x.String01 == "Category00_050" && x.Double001 > 0.5 && x.Int001 > 1000)
            .ToList().Count;
    }

    #endregion

    #region Any on Wide Records - Early termination benefit

    /// <summary>
    /// Any with List on wide records.
    /// </summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("WideRecord_Any")]
    public bool List_WideRecord_Any()
    {
        return _list.Where(x => x.String02 == "Category01_099").Any();
    }

    /// <summary>
    /// Any with ArrowCollection - may need to reconstruct many items before finding match.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("WideRecord_Any")]
    public bool ArrowCollection_WideRecord_Any()
    {
        return _arrowCollection.Where(x => x.String02 == "Category01_099").Any();
    }

    /// <summary>
    /// Any with ArrowQuery - scans column, reconstructs only if found.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("WideRecord_Any")]
    public bool ArrowQuery_WideRecord_Any()
    {
        return _arrowCollection.AsQueryable().Where(x => x.String02 == "Category01_099").Any();
    }

    #endregion
}
