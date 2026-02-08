using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using DuckDB.NET.Data;
using FrozenArrow.Query;

namespace FrozenArrow.Benchmarks;

/// <summary>
/// Benchmarks comparing SQL queries vs LINQ equivalents in FrozenArrow.
/// Validates that SQL support doesn't sacrifice performance.
/// </summary>
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[GroupBenchmarksBy(BenchmarkDotNet.Configs.BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
[ShortRunJob]
public class SqlBenchmarks
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

    #region Simple WHERE Clause

    [Benchmark]
    [BenchmarkCategory("SimpleWhere")]
    public int FrozenArrow_LINQ_SimpleWhere()
    {
        return _frozenArrow.AsQueryable().Where(x => x.Age > 30).Count();
    }

    [Benchmark]
    [BenchmarkCategory("SimpleWhere")]
    public int FrozenArrow_SQL_SimpleWhere()
    {
        return _frozenArrow.ExecuteSql<QueryBenchmarkItem, QueryBenchmarkItem>("SELECT * FROM items WHERE Age > 30").Count();
    }

    [Benchmark]
    [BenchmarkCategory("SimpleWhere")]
    public int DuckDB_SQL_SimpleWhere()
    {
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM items WHERE Age > 30";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    #endregion

    #region Complex WHERE with AND

    [Benchmark]
    [BenchmarkCategory("ComplexAnd")]
    public int FrozenArrow_LINQ_ComplexAnd()
    {
        return _frozenArrow.AsQueryable()
            .Where(x => x.Age > 30 && x.IsActive && x.Salary > 60000)
            .Count();
    }

    [Benchmark]
    [BenchmarkCategory("ComplexAnd")]
    public int FrozenArrow_SQL_ComplexAnd()
    {
        return _frozenArrow.ExecuteSql<QueryBenchmarkItem, QueryBenchmarkItem>(
            "SELECT * FROM items WHERE Age > 30 AND IsActive = true AND Salary > 60000"
        ).Count();
    }

    [Benchmark]
    [BenchmarkCategory("ComplexAnd")]
    public int DuckDB_SQL_ComplexAnd()
    {
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM items WHERE Age > 30 AND IsActive = true AND Salary > 60000";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    #endregion

    #region Complex WHERE with OR

    [Benchmark]
    [BenchmarkCategory("ComplexOr")]
    public int FrozenArrow_LINQ_ComplexOr()
    {
        return _frozenArrow.AsQueryable()
            .Where(x => x.Age < 25 || x.Age > 55 || x.Category == "Executive")
            .Count();
    }

    [Benchmark]
    [BenchmarkCategory("ComplexOr")]
    public int FrozenArrow_SQL_ComplexOr()
    {
        return _frozenArrow.ExecuteSql<QueryBenchmarkItem, QueryBenchmarkItem>(
            "SELECT * FROM items WHERE Age < 25 OR Age > 55 OR Category = 'Executive'"
        ).Count();
    }

    [Benchmark]
    [BenchmarkCategory("ComplexOr")]
    public int DuckDB_SQL_ComplexOr()
    {
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM items WHERE Age < 25 OR Age > 55 OR Category = 'Executive'";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    #endregion
}
