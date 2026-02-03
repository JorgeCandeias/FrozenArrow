using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using DuckDB.NET.Data;
using FrozenArrow.Query;

namespace FrozenArrow.Benchmarks;

/// <summary>
/// Benchmarks for aggregation operations (Sum, Average, Min, Max) across all technologies.
/// Tests both dense and sparse selections.
/// </summary>
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[GroupBenchmarksBy(BenchmarkDotNet.Configs.BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
[ShortRunJob]
public class AggregationBenchmarks
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

    #region Sum (Filtered ~70% selectivity)

    [Benchmark]
    [BenchmarkCategory("Sum")]
    public decimal List_Sum()
    {
        return _list.Where(x => x.IsActive).Sum(x => x.Salary);
    }

    [Benchmark]
    [BenchmarkCategory("Sum")]
    public decimal FrozenArrow_Sum()
    {
        return _frozenArrow.AsQueryable().Where(x => x.IsActive).Sum(x => x.Salary);
    }

    [Benchmark]
    [BenchmarkCategory("Sum")]
    public decimal DuckDB_Sum()
    {
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT SUM(Salary) FROM items WHERE IsActive = true";
        return Convert.ToDecimal(cmd.ExecuteScalar());
    }

    #endregion

    #region Average (Filtered)

    [Benchmark]
    [BenchmarkCategory("Average")]
    public double List_Average()
    {
        return _list.Where(x => x.Age > 30).Average(x => x.PerformanceScore);
    }

    [Benchmark]
    [BenchmarkCategory("Average")]
    public double FrozenArrow_Average()
    {
        return _frozenArrow.AsQueryable().Where(x => x.Age > 30).Average(x => x.PerformanceScore);
    }

    [Benchmark]
    [BenchmarkCategory("Average")]
    public double DuckDB_Average()
    {
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT AVG(PerformanceScore) FROM items WHERE Age > 30";
        return Convert.ToDouble(cmd.ExecuteScalar());
    }

    #endregion

    #region Min (Filtered)

    [Benchmark]
    [BenchmarkCategory("Min")]
    public decimal List_Min()
    {
        return _list.Where(x => x.Age > 40).Min(x => x.Salary);
    }

    [Benchmark]
    [BenchmarkCategory("Min")]
    public decimal FrozenArrow_Min()
    {
        return _frozenArrow.AsQueryable().Where(x => x.Age > 40).Min(x => x.Salary);
    }

    [Benchmark]
    [BenchmarkCategory("Min")]
    public decimal DuckDB_Min()
    {
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT MIN(Salary) FROM items WHERE Age > 40";
        return Convert.ToDecimal(cmd.ExecuteScalar());
    }

    #endregion

    #region Max (Filtered)

    [Benchmark]
    [BenchmarkCategory("Max")]
    public decimal List_Max()
    {
        return _list.Where(x => x.Age > 40).Max(x => x.Salary);
    }

    [Benchmark]
    [BenchmarkCategory("Max")]
    public decimal FrozenArrow_Max()
    {
        return _frozenArrow.AsQueryable().Where(x => x.Age > 40).Max(x => x.Salary);
    }

    [Benchmark]
    [BenchmarkCategory("Max")]
    public decimal DuckDB_Max()
    {
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT MAX(Salary) FROM items WHERE Age > 40";
        return Convert.ToDecimal(cmd.ExecuteScalar());
    }

    #endregion
}
