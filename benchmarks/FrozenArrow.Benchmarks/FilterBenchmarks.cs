using FrozenArrow.Query;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using DuckDB.NET.Data;

namespace FrozenArrow.Benchmarks;

/// <summary>
/// Benchmarks for filter operations (Where clauses) across all technologies.
/// Tests various selectivities and filter types.
/// </summary>
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[GroupBenchmarksBy(BenchmarkDotNet.Configs.BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
[ShortRunJob]
public class FilterBenchmarks
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

    #region Filter + Count (High Selectivity ~5%)

    [Benchmark]
    [BenchmarkCategory("Filter_Count_HighSelectivity")]
    public int List_Filter_Count_HighSelectivity()
    {
        return _list.Where(x => x.Age > 55).Count();
    }

    [Benchmark]
    [BenchmarkCategory("Filter_Count_HighSelectivity")]
    public int FrozenArrow_Filter_Count_HighSelectivity()
    {
        return _frozenArrow.AsQueryable().Where(x => x.Age > 55).Count();
    }

    [Benchmark]
    [BenchmarkCategory("Filter_Count_HighSelectivity")]
    public int DuckDB_Filter_Count_HighSelectivity()
    {
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM items WHERE Age > 55";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    #endregion

    #region Filter + Count (Low Selectivity ~70%)

    [Benchmark]
    [BenchmarkCategory("Filter_Count_LowSelectivity")]
    public int List_Filter_Count_LowSelectivity()
    {
        return _list.Where(x => x.IsActive).Count();
    }

    [Benchmark]
    [BenchmarkCategory("Filter_Count_LowSelectivity")]
    public int FrozenArrow_Filter_Count_LowSelectivity()
    {
        return _frozenArrow.AsQueryable().Where(x => x.IsActive).Count();
    }

    [Benchmark]
    [BenchmarkCategory("Filter_Count_LowSelectivity")]
    public int DuckDB_Filter_Count_LowSelectivity()
    {
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM items WHERE IsActive = true";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    #endregion

    #region Filter + ToList (High Selectivity ~5%)

    [Benchmark]
    [BenchmarkCategory("Filter_ToList_HighSelectivity")]
    public int List_Filter_ToList_HighSelectivity()
    {
        return _list.Where(x => x.Age > 55).ToList().Count;
    }

    [Benchmark]
    [BenchmarkCategory("Filter_ToList_HighSelectivity")]
    public int FrozenArrow_Filter_ToList_HighSelectivity()
    {
        return _frozenArrow.AsQueryable().Where(x => x.Age > 55).ToList().Count;
    }

    [Benchmark]
    [BenchmarkCategory("Filter_ToList_HighSelectivity")]
    public int DuckDB_Filter_ToList_HighSelectivity()
    {
        var results = new List<QueryBenchmarkItem>();
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT * FROM items WHERE Age > 55";
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            results.Add(new QueryBenchmarkItem
            {
                Id = reader.GetInt32(0),
                Name = reader.GetString(1),
                Age = reader.GetInt32(2),
                Salary = reader.GetDecimal(3),
                IsActive = reader.GetBoolean(4),
                Category = reader.GetString(5),
                Department = reader.GetString(6),
                HireDate = reader.GetDateTime(7),
                PerformanceScore = reader.GetDouble(8),
                Region = reader.GetString(9)
            });
        }
        return results.Count;
    }

    #endregion

    #region Compound Filter (3 conditions)

    [Benchmark]
    [BenchmarkCategory("Filter_Compound")]
    public int List_Filter_Compound()
    {
        return _list.Where(x => x.Age > 30 && x.IsActive && x.Category == "Engineering").Count();
    }

    [Benchmark]
    [BenchmarkCategory("Filter_Compound")]
    public int FrozenArrow_Filter_Compound()
    {
        return _frozenArrow.AsQueryable()
            .Where(x => x.Age > 30 && x.IsActive && x.Category == "Engineering")
            .Count();
    }

    [Benchmark]
    [BenchmarkCategory("Filter_Compound")]
    public int DuckDB_Filter_Compound()
    {
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM items WHERE Age > 30 AND IsActive = true AND Category = 'Engineering'";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    #endregion

    #region String Equality Filter

    [Benchmark]
    [BenchmarkCategory("Filter_StringEquality")]
    public int List_Filter_StringEquality()
    {
        return _list.Where(x => x.Category == "Engineering").Count();
    }

    [Benchmark]
    [BenchmarkCategory("Filter_StringEquality")]
    public int FrozenArrow_Filter_StringEquality()
    {
        return _frozenArrow.AsQueryable().Where(x => x.Category == "Engineering").Count();
    }

    [Benchmark]
    [BenchmarkCategory("Filter_StringEquality")]
    public int DuckDB_Filter_StringEquality()
    {
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM items WHERE Category = 'Engineering'";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    #endregion

    #region String Contains Filter

    [Benchmark]
    [BenchmarkCategory("Filter_StringContains")]
    public int List_Filter_StringContains()
    {
        return _list.Where(x => x.Name.Contains("42")).Count();
    }

    [Benchmark]
    [BenchmarkCategory("Filter_StringContains")]
    public int FrozenArrow_Filter_StringContains()
    {
        return _frozenArrow.AsQueryable().Where(x => x.Name.Contains("42")).Count();
    }

    [Benchmark]
    [BenchmarkCategory("Filter_StringContains")]
    public int DuckDB_Filter_StringContains()
    {
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM items WHERE Name LIKE '%42%'";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    #endregion

    #region Decimal Comparison Filter

    [Benchmark]
    [BenchmarkCategory("Filter_DecimalComparison")]
    public int List_Filter_DecimalComparison()
    {
        return _list.Where(x => x.Salary > 75000m).Count();
    }

    [Benchmark]
    [BenchmarkCategory("Filter_DecimalComparison")]
    public int FrozenArrow_Filter_DecimalComparison()
    {
        return _frozenArrow.AsQueryable().Where(x => x.Salary > 75000m).Count();
    }

    [Benchmark]
    [BenchmarkCategory("Filter_DecimalComparison")]
    public int DuckDB_Filter_DecimalComparison()
    {
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM items WHERE Salary > 75000";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    #endregion
}
