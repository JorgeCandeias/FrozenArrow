using ArrowCollection.Query;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;

namespace ArrowCollection.Benchmarks;

/// <summary>
/// Comprehensive LINQ query benchmarks at 1 million items scale.
/// Compares List&lt;T&gt; LINQ-to-Objects baseline against ArrowQuery optimizations.
/// 
/// Categories:
/// - Filtering: Where clauses with various selectivities
/// - Aggregates: Sum, Average, Min, Max (single aggregate)
/// - GroupBy: Grouped aggregations
/// - MultiAggregate: Multiple aggregates in single pass
/// </summary>
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[GroupBenchmarksBy(BenchmarkDotNet.Configs.BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
public class LargeScaleQueryBenchmarks
{
    private List<QueryBenchmarkItem> _list = null!;
    private ArrowCollection<QueryBenchmarkItem> _arrowCollection = null!;

    /// <summary>
    /// 1 million items for large-scale benchmark.
    /// </summary>
    [Params(1_000_000)]
    public int ItemCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _list = QueryBenchmarkItemFactory.Generate(ItemCount);
        _arrowCollection = _list.ToArrowCollection();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _arrowCollection.Dispose();
    }

    #region Filter + Count (No Materialization)

    /// <summary>
    /// List baseline: Count with filter (~30% selectivity).
    /// </summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("FilterCount")]
    public int List_FilterCount()
    {
        return _list.Where(x => x.Age > 40).Count();
    }

    /// <summary>
    /// ArrowQuery: Count uses popcount on bitmap, no materialization.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("FilterCount")]
    public int ArrowQuery_FilterCount()
    {
        return _arrowCollection.AsQueryable().Where(x => x.Age > 40).Count();
    }

    #endregion

    #region Filter + ToList (Full Materialization)

    /// <summary>
    /// List baseline: Filter and materialize results (~5% selectivity).
    /// </summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("FilterToList")]
    public int List_FilterToList()
    {
        return _list.Where(x => x.Age > 55).ToList().Count;
    }

    /// <summary>
    /// ArrowQuery: Only materializes matching rows.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("FilterToList")]
    public int ArrowQuery_FilterToList()
    {
        return _arrowCollection.AsQueryable().Where(x => x.Age > 55).ToList().Count;
    }

    #endregion

    #region Single Aggregates (Sum)

    /// <summary>
    /// List baseline: Sum after filter.
    /// </summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Sum")]
    public decimal List_Sum()
    {
        return _list.Where(x => x.IsActive).Sum(x => x.Salary);
    }

    /// <summary>
    /// ArrowQuery: Column-level sum, no object materialization.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Sum")]
    public decimal ArrowQuery_Sum()
    {
        return _arrowCollection.AsQueryable().Where(x => x.IsActive).Sum(x => x.Salary);
    }

    #endregion

    #region Single Aggregates (Average)

    /// <summary>
    /// List baseline: Average after filter.
    /// </summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Average")]
    public double List_Average()
    {
        return _list.Where(x => x.IsActive).Average(x => x.Age);
    }

    /// <summary>
    /// ArrowQuery: Column-level average.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Average")]
    public double ArrowQuery_Average()
    {
        return _arrowCollection.AsQueryable().Where(x => x.IsActive).Average(x => x.Age);
    }

    #endregion

    #region Single Aggregates (Min/Max)

    /// <summary>
    /// List baseline: Min after filter (using Age filter instead of Category to avoid dictionary-encoded columns).
    /// </summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("MinMax")]
    public decimal List_Min()
    {
        return _list.Where(x => x.Age > 40).Min(x => x.Salary);
    }

    /// <summary>
    /// ArrowQuery: Column-level min.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("MinMax")]
    public decimal ArrowQuery_Min()
    {
        return _arrowCollection.AsQueryable().Where(x => x.Age > 40).Min(x => x.Salary);
    }

    #endregion

    #region GroupBy with Count

    /// <summary>
    /// List baseline: GroupBy Age (integer column, ~41 groups) and count per group.
    /// Using integer column to avoid dictionary-encoding complexities.
    /// </summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("GroupByCount")]
    public int List_GroupByCount()
    {
        return _list
            .GroupBy(x => x.Age)
            .Select(g => new { Age = g.Key, Count = g.Count() })
            .ToList().Count;
    }

    /// <summary>
    /// ArrowQuery: Column-level grouping on integer column.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("GroupByCount")]
    public int ArrowQuery_GroupByCount()
    {
        return _arrowCollection
            .AsQueryable()
            .GroupBy(x => x.Age)
            .Select(g => new { Age = g.Key, Count = g.Count() })
            .ToList().Count;
    }

    #endregion

    #region GroupBy with Sum

    /// <summary>
    /// List baseline: GroupBy Age and sum salaries.
    /// </summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("GroupBySum")]
    public decimal List_GroupBySum()
    {
        return _list
            .GroupBy(x => x.Age)
            .Select(g => new { Age = g.Key, Total = g.Sum(x => x.Salary) })
            .Sum(x => x.Total);
    }

    /// <summary>
    /// ArrowQuery: Column-level group-sum on integer key.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("GroupBySum")]
    public decimal ArrowQuery_GroupBySum()
    {
        return _arrowCollection
            .AsQueryable()
            .GroupBy(x => x.Age)
            .Select(g => new { Age = g.Key, Total = g.Sum(x => x.Salary) })
            .Sum(x => x.Total);
    }

    #endregion

    #region GroupBy with Multiple Aggregates

    /// <summary>
    /// List baseline: GroupBy with Count + Sum + Average.
    /// </summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("GroupByMulti")]
    public int List_GroupByMultipleAggregates()
    {
        return _list
            .GroupBy(x => x.Age)
            .Select(g => new 
            { 
                Age = g.Key, 
                Count = g.Count(),
                TotalSalary = g.Sum(x => x.Salary),
                AvgPerformance = g.Average(x => x.PerformanceScore)
            })
            .ToList().Count;
    }

    /// <summary>
    /// ArrowQuery: Multiple aggregates per group in single pass.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("GroupByMulti")]
    public int ArrowQuery_GroupByMultipleAggregates()
    {
        return _arrowCollection
            .AsQueryable()
            .GroupBy(x => x.Age)
            .Select(g => new 
            { 
                Age = g.Key, 
                Count = g.Count(),
                TotalSalary = g.Sum(x => x.Salary),
                AvgPerformance = g.Average(x => x.PerformanceScore)
            })
            .ToList().Count;
    }

    #endregion

    #region GroupBy with Filter

    /// <summary>
    /// List baseline: Filter then GroupBy.
    /// </summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("FilterGroupBy")]
    public int List_FilterGroupBy()
    {
        return _list
            .Where(x => x.IsActive)
            .GroupBy(x => x.Age)
            .Select(g => new { Age = g.Key, Count = g.Count() })
            .ToList().Count;
    }

    /// <summary>
    /// ArrowQuery: Filter with bitmap, then group on integer column.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("FilterGroupBy")]
    public int ArrowQuery_FilterGroupBy()
    {
        return _arrowCollection
            .AsQueryable()
            .Where(x => x.IsActive)
            .GroupBy(x => x.Age)
            .Select(g => new { Age = g.Key, Count = g.Count() })
            .ToList().Count;
    }

    #endregion

    #region Multi-Aggregate (Phase 3) - No GroupBy

    /// <summary>
    /// Result type for multi-aggregate benchmark.
    /// </summary>
    public class SalaryStats
    {
        public decimal TotalSalary { get; set; }
        public double AverageAge { get; set; }
        public decimal MinSalary { get; set; }
        public decimal MaxSalary { get; set; }
        public int Count { get; set; }
    }

    /// <summary>
    /// List baseline: Multiple separate aggregate calls (5 passes).
    /// </summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("MultiAggregate")]
    public SalaryStats List_MultiAggregate()
    {
        var filtered = _list.Where(x => x.IsActive).ToList();
        return new SalaryStats
        {
            TotalSalary = filtered.Sum(x => x.Salary),
            AverageAge = filtered.Average(x => x.Age),
            MinSalary = filtered.Min(x => x.Salary),
            MaxSalary = filtered.Max(x => x.Salary),
            Count = filtered.Count
        };
    }

    /// <summary>
    /// ArrowQuery: Single-pass multi-aggregate.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("MultiAggregate")]
    public SalaryStats ArrowQuery_MultiAggregate()
    {
        return _arrowCollection
            .AsQueryable()
            .Where(x => x.IsActive)
            .Aggregate(agg => new SalaryStats
            {
                TotalSalary = agg.Sum(x => x.Salary),
                AverageAge = agg.Average(x => x.Age),
                MinSalary = agg.Min(x => x.Salary),
                MaxSalary = agg.Max(x => x.Salary),
                Count = agg.Count()
            });
    }

    #endregion

    #region Complex Multi-Predicate

    /// <summary>
    /// List baseline: Complex filter with multiple conditions.
    /// </summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("ComplexFilter")]
    public int List_ComplexFilter()
    {
        return _list
            .Where(x => x.Age > 30 && x.Age < 50 && x.IsActive && x.Category == "Engineering")
            .Count();
    }

    /// <summary>
    /// ArrowQuery: Complex filter evaluated column-by-column.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("ComplexFilter")]
    public int ArrowQuery_ComplexFilter()
    {
        return _arrowCollection
            .AsQueryable()
            .Where(x => x.Age > 30 && x.Age < 50 && x.IsActive && x.Category == "Engineering")
            .Count();
    }

    #endregion
}
