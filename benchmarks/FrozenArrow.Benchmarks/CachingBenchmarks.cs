using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using FrozenArrow.Query;

namespace FrozenArrow.Benchmarks;

/// <summary>
/// Benchmarks demonstrating query plan caching effectiveness.
/// Shows improvement for repeated queries with caching enabled.
/// </summary>
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[GroupBenchmarksBy(BenchmarkDotNet.Configs.BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
[ShortRunJob]
public class CachingBenchmarks
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

    #region Repeated Execution (10 times)

    [Benchmark]
    [BenchmarkCategory("RepeatedExecution")]
    public int List_Repeated_SimpleFilter()
    {
        int result = 0;
        for (int i = 0; i < 10; i++)
        {
            result = _list.Where(x => x.Age > 30).Count();
        }
        return result;
    }

    [Benchmark]
    [BenchmarkCategory("RepeatedExecution")]
    public int FrozenArrow_Repeated_SimpleFilter()
    {
        int result = 0;
        for (int i = 0; i < 10; i++)
        {
            result = _frozenArrow.AsQueryable().Where(x => x.Age > 30).Count();
        }
        return result;
    }

    #endregion

    #region Complex Query - Repeated

    [Benchmark]
    [BenchmarkCategory("ComplexQuery")]
    public decimal List_Complex_Repeated()
    {
        decimal result = 0;
        for (int i = 0; i < 5; i++)
        {
            result = _list
                .Where(x => x.Age > 30 && x.IsActive)
                .Where(x => x.Salary > 60000)
                .Sum(x => x.Salary);
        }
        return result;
    }

    [Benchmark]
    [BenchmarkCategory("ComplexQuery")]
    public decimal FrozenArrow_Complex_Repeated()
    {
        decimal result = 0;
        for (int i = 0; i < 5; i++)
        {
            result = _frozenArrow.AsQueryable()
                .Where(x => x.Age > 30 && x.IsActive)
                .Where(x => x.Salary > 60000)
                .Sum(x => x.Salary);
        }
        return result;
    }

    #endregion

    #region SQL Query Caching

    [Benchmark]
    [BenchmarkCategory("SqlCaching")]
    public int List_Sql_NoCache()
    {
        // List doesn't support SQL, so just use LINQ
        int result = 0;
        for (int i = 0; i < 10; i++)
        {
            result = _list.Where(x => x.Age > 30 && x.IsActive).Count();
        }
        return result;
    }

    [Benchmark]
    [BenchmarkCategory("SqlCaching")]
    public int FrozenArrow_Sql_WithCache()
    {
        // SQL query should be parsed once, then cached
        int result = 0;
        for (int i = 0; i < 10; i++)
        {
            result = _frozenArrow.ExecuteSql<QueryBenchmarkItem, QueryBenchmarkItem>(
                "SELECT * FROM items WHERE Age > 30 AND IsActive = true"
            ).Count();
        }
        return result;
    }

    #endregion

    #region GroupBy Caching

    [Benchmark]
    [BenchmarkCategory("GroupByCache")]
    public Dictionary<string, int> List_GroupBy_Repeated()
    {
        Dictionary<string, int> result = null!;
        for (int i = 0; i < 5; i++)
        {
            result = _list.GroupBy(x => x.Category).ToDictionary(g => g.Key, g => g.Count());
        }
        return result;
    }

    [Benchmark]
    [BenchmarkCategory("GroupByCache")]
    public Dictionary<string, int> FrozenArrow_GroupBy_Repeated()
    {
        Dictionary<string, int> result = null!;
        for (int i = 0; i < 5; i++)
        {
            result = _frozenArrow.AsQueryable()
                .GroupBy(x => x.Category)
                .ToDictionary(g => g.Key, g => g.Count());
        }
        return result;
    }

    #endregion
}
