using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using FrozenArrow.Query;

namespace FrozenArrow.Benchmarks.Internals;

/// <summary>
/// Benchmarks demonstrating Zone Map (min-max index) optimization.
/// Zone maps allow skipping entire chunks when predicates exclude them based on min/max values.
/// </summary>
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[ShortRunJob]
public class ZoneMapBenchmarks
{
    private List<ZoneMapTestItem> _sortedList = null!;
    private List<ZoneMapTestItem> _randomList = null!;
    private FrozenArrow<ZoneMapTestItem> _sortedFrozen = null!;
    private FrozenArrow<ZoneMapTestItem> _randomFrozen = null!;

    [Params(100_000, 1_000_000)]
    public int ItemCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        // Create sorted data (ideal for zone maps - consecutive chunks have non-overlapping ranges)
        _sortedList = Enumerable.Range(1, ItemCount)
            .Select(i => new ZoneMapTestItem
            {
                Id = i,
                Value = i, // Sequential values
                Category = $"Cat{i / 10000}", // Groups for GroupBy tests
                Score = i % 100 + (double)i / ItemCount
            })
            .ToList();

        _sortedFrozen = _sortedList.ToFrozenArrow();

        // Create random data (less ideal for zone maps - chunks have overlapping ranges)
        var random = new Random(42);
        _randomList = Enumerable.Range(1, ItemCount)
            .Select(i => new ZoneMapTestItem
            {
                Id = i,
                Value = random.Next(1, ItemCount * 10), // Random values
                Category = $"Cat{random.Next(0, 100)}",
                Score = random.NextDouble() * 100
            })
            .ToList();

        _randomFrozen = _randomList.ToFrozenArrow();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _sortedFrozen?.Dispose();
        _randomFrozen?.Dispose();
    }

    #region Highly Selective Query on Sorted Data (Best Case for Zone Maps)

    /// <summary>
    /// Best case: Sorted data with highly selective predicate at the end.
    /// Zone maps will skip most chunks entirely.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("HighlySelective_Sorted")]
    public int List_Sorted_HighlySelective()
    {
        // Select only top 1% of values
        var threshold = ItemCount * 99 / 100;
        return _sortedList.Where(x => x.Value > threshold).Count();
    }

    [Benchmark]
    [BenchmarkCategory("HighlySelective_Sorted")]
    public int FrozenArrow_Sorted_HighlySelective()
    {
        var threshold = ItemCount * 99 / 100;
        return _sortedFrozen.AsQueryable().Where(x => x.Value > threshold).Count();
    }

    #endregion

    #region Highly Selective Query on Random Data (Moderate Benefit)

    /// <summary>
    /// Moderate case: Random data with highly selective predicate.
    /// Zone maps will skip some chunks but not as many as sorted data.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("HighlySelective_Random")]
    public int List_Random_HighlySelective()
    {
        var threshold = ItemCount * 99 / 10; // Very high threshold
        return _randomList.Where(x => x.Value > threshold).Count();
    }

    [Benchmark]
    [BenchmarkCategory("HighlySelective_Random")]
    public int FrozenArrow_Random_HighlySelective()
    {
        var threshold = ItemCount * 99 / 10;
        return _randomFrozen.AsQueryable().Where(x => x.Value > threshold).Count();
    }

    #endregion

    #region Range Query on Sorted Data (Excellent for Zone Maps)

    /// <summary>
    /// Range query on sorted data - can skip chunks before and after the range.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Range_Sorted")]
    public int List_Sorted_Range()
    {
        var min = ItemCount * 40 / 100;
        var max = ItemCount * 60 / 100;
        return _sortedList.Where(x => x.Value >= min && x.Value <= max).Count();
    }

    [Benchmark]
    [BenchmarkCategory("Range_Sorted")]
    public int FrozenArrow_Sorted_Range()
    {
        var min = ItemCount * 40 / 100;
        var max = ItemCount * 60 / 100;
        return _sortedFrozen.AsQueryable().Where(x => x.Value >= min && x.Value <= max).Count();
    }

    #endregion

    #region Filtered Aggregation (Zone Maps + Fused Execution)

    /// <summary>
    /// Filtered aggregation on sorted data - combines zone map skip with fused execution.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("FilteredAggregate_Sorted")]
    public double List_Sorted_FilteredSum()
    {
        var threshold = ItemCount * 80 / 100;
        return _sortedList.Where(x => x.Value > threshold).Sum(x => x.Score);
    }

    [Benchmark]
    [BenchmarkCategory("FilteredAggregate_Sorted")]
    public double FrozenArrow_Sorted_FilteredSum()
    {
        var threshold = ItemCount * 80 / 100;
        return _sortedFrozen.AsQueryable().Where(x => x.Value > threshold).Sum(x => x.Score);
    }

    #endregion

    #region Multiple Predicates (AND - Zone Maps Help)

    /// <summary>
    /// Multiple predicates with AND - if ANY predicate excludes a chunk, skip it.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("MultiplePredicate_Sorted")]
    public int List_Sorted_MultiplePredicate()
    {
        var minValue = ItemCount * 70 / 100;
        var minScore = 80.0;
        return _sortedList.Where(x => x.Value > minValue && x.Score > minScore).Count();
    }

    [Benchmark]
    [BenchmarkCategory("MultiplePredicate_Sorted")]
    public int FrozenArrow_Sorted_MultiplePredicate()
    {
        var minValue = ItemCount * 70 / 100;
        var minScore = 80.0;
        return _sortedFrozen.AsQueryable().Where(x => x.Value > minValue && x.Score > minScore).Count();
    }

    #endregion

    #region Low Selectivity (Zone Maps Have Little Effect)

    /// <summary>
    /// Low selectivity query - most chunks contain matches, zone maps can't skip much.
    /// This shows the overhead of zone map checking is minimal.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("LowSelective_Sorted")]
    public int List_Sorted_LowSelective()
    {
        var threshold = ItemCount * 10 / 100;
        return _sortedList.Where(x => x.Value > threshold).Count();
    }

    [Benchmark]
    [BenchmarkCategory("LowSelective_Sorted")]
    public int FrozenArrow_Sorted_LowSelective()
    {
        var threshold = ItemCount * 10 / 100;
        return _sortedFrozen.AsQueryable().Where(x => x.Value > threshold).Count();
    }

    #endregion
}

public class ZoneMapTestItem
{
    public int Id { get; set; }
    public int Value { get; set; }
    public string Category { get; set; } = "";
    public double Score { get; set; }
}
