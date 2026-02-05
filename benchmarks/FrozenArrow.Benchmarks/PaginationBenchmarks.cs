using FrozenArrow.Query;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using DuckDB.NET.Data;

namespace FrozenArrow.Benchmarks;

/// <summary>
/// Benchmarks for pagination operations (Take, Skip, First, Any) across all technologies.
/// </summary>
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[GroupBenchmarksBy(BenchmarkDotNet.Configs.BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
[ShortRunJob]
public class PaginationBenchmarks
{
    private List<QueryBenchmarkItem> _list = null!;
    private FrozenArrow<QueryBenchmarkItem> _frozenArrow = null!;
    private DuckDBConnection _duckDbConnection = null!;

    [Params(10_000, 100_000, 1_000_000)]
    public int ItemCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _list = QueryBenchmarkItemFactory.Generate(ItemCount);
        _frozenArrow = _list.ToFrozenArrow();

        // Setup in-memory DuckDB
        _duckDbConnection = new DuckDBConnection("DataSource=:memory:");
        _duckDbConnection.Open();

        using var createCmd = _duckDbConnection.CreateCommand();
        createCmd.CommandText = """
            CREATE TABLE items (
                Id INTEGER,
                Name VARCHAR,
                Age INTEGER,
                Salary DECIMAL(18,2),
                IsActive BOOLEAN,
                Category VARCHAR,
                Department VARCHAR,
                HireDate TIMESTAMP,
                PerformanceScore DOUBLE,
                Region VARCHAR
            )
            """;
        createCmd.ExecuteNonQuery();

        using var appender = _duckDbConnection.CreateAppender("items");
        foreach (var item in _list)
        {
            var row = appender.CreateRow();
            row.AppendValue(item.Id);
            row.AppendValue(item.Name);
            row.AppendValue(item.Age);
            row.AppendValue(item.Salary);
            row.AppendValue(item.IsActive);
            row.AppendValue(item.Category);
            row.AppendValue(item.Department);
            row.AppendValue(item.HireDate);
            row.AppendValue(item.PerformanceScore);
            row.AppendValue(item.Region);
            row.EndRow();
        }
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _frozenArrow.Dispose();
        _duckDbConnection.Dispose();
    }

    #region Any (Short-circuit evaluation)

    [Benchmark]
    [BenchmarkCategory("Any")]
    public bool List_Any()
    {
        return _list.Any(x => x.Age > 55 && x.Category == "Executive");
    }

    [Benchmark]
    [BenchmarkCategory("Any")]
    public bool FrozenArrow_Any()
    {
        return _frozenArrow.AsQueryable().Any(x => x.Age > 55 && x.Category == "Executive");
    }

    [Benchmark]
    [BenchmarkCategory("Any")]
    public bool DuckDB_Any()
    {
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT EXISTS(SELECT 1 FROM items WHERE Age > 55 AND Category = 'Executive')";
        return Convert.ToBoolean(cmd.ExecuteScalar());
    }

    #endregion

    #region First (Stop at first match)

    [Benchmark]
    [BenchmarkCategory("First")]
    public int List_First()
    {
        return _list.First(x => x.Age > 55).Id;
    }

    [Benchmark]
    [BenchmarkCategory("First")]
    public int FrozenArrow_First()
    {
        return _frozenArrow.AsQueryable().First(x => x.Age > 55).Id;
    }

    [Benchmark]
    [BenchmarkCategory("First")]
    public int DuckDB_First()
    {
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT Id FROM items WHERE Age > 55 LIMIT 1";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    #endregion

    #region Take (Limit results)

    [Benchmark]
    [BenchmarkCategory("Take")]
    public int List_Take()
    {
        return _list.Where(x => x.IsActive).Take(100).Count();
    }

    [Benchmark]
    [BenchmarkCategory("Take")]
    public int FrozenArrow_Take()
    {
        return _frozenArrow.AsQueryable().Where(x => x.IsActive).Take(100).Count();
    }

    [Benchmark]
    [BenchmarkCategory("Take")]
    public int DuckDB_Take()
    {
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM (SELECT 1 FROM items WHERE IsActive = true LIMIT 100)";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    #endregion

    #region Skip + Take (Pagination)

    [Benchmark]
    [BenchmarkCategory("SkipTake")]
    public int List_SkipTake()
    {
        return _list.Where(x => x.IsActive).Skip(1000).Take(100).Count();
    }

    [Benchmark]
    [BenchmarkCategory("SkipTake")]
    public int FrozenArrow_SkipTake()
    {
        return _frozenArrow.AsQueryable().Where(x => x.IsActive).Skip(1000).Take(100).Count();
    }

    [Benchmark]
    [BenchmarkCategory("SkipTake")]
    public int DuckDB_SkipTake()
    {
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM (SELECT 1 FROM items WHERE IsActive = true LIMIT 100 OFFSET 1000)";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    #endregion

    #region Take with Materialization (ToList)

    [Benchmark]
    [BenchmarkCategory("TakeMaterialize")]
    public int List_TakeMaterialize()
    {
        return _list.Where(x => x.IsActive).Take(100).ToList().Count;
    }

    [Benchmark]
    [BenchmarkCategory("TakeMaterialize")]
    public int FrozenArrow_TakeMaterialize()
    {
        return _frozenArrow.AsQueryable().Where(x => x.IsActive).Take(100).ToList().Count;
    }

    [Benchmark]
    [BenchmarkCategory("TakeMaterialize")]
    public int DuckDB_TakeMaterialize()
    {
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT * FROM items WHERE IsActive = true LIMIT 100";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    #endregion

    #region Large Skip (Page Deep into Results)

    [Benchmark]
    [BenchmarkCategory("LargeSkip")]
    public int List_LargeSkip()
    {
        return _list.Where(x => x.Age > 25).Skip(50000).Take(10).Count();
    }

    [Benchmark]
    [BenchmarkCategory("LargeSkip")]
    public int FrozenArrow_LargeSkip()
    {
        return _frozenArrow.AsQueryable().Where(x => x.Age > 25).Skip(50000).Take(10).Count();
    }

    [Benchmark]
    [BenchmarkCategory("LargeSkip")]
    public int DuckDB_LargeSkip()
    {
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM (SELECT 1 FROM items WHERE Age > 25 LIMIT 10 OFFSET 50000)";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    #endregion
}
