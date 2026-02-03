using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using DuckDB.NET.Data;
using FrozenArrow.Query;

namespace FrozenArrow.Benchmarks;

/// <summary>
/// Benchmarks for GroupBy operations with aggregations across all technologies.
/// Tests various group cardinalities and aggregate types.
/// </summary>
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[GroupBenchmarksBy(BenchmarkDotNet.Configs.BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
[ShortRunJob]
public class GroupByBenchmarks
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

    #region GroupBy + Count (Low Cardinality ~8 groups)

    [Benchmark]
    [BenchmarkCategory("GroupBy_Count")]
    public Dictionary<string, int> List_GroupBy_Count()
    {
        return _list
            .GroupBy(x => x.Category)
            .ToDictionary(g => g.Key, g => g.Count());
    }

    [Benchmark]
    [BenchmarkCategory("GroupBy_Count")]
    public Dictionary<string, int> FrozenArrow_GroupBy_Count()
    {
        return _frozenArrow.AsQueryable()
            .GroupBy(x => x.Category)
            .ToDictionary(g => g.Key, g => g.Count());
    }

    [Benchmark]
    [BenchmarkCategory("GroupBy_Count")]
    public Dictionary<string, int> DuckDB_GroupBy_Count()
    {
        var result = new Dictionary<string, int>();
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT Category, COUNT(*) FROM items GROUP BY Category";
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            result[reader.GetString(0)] = Convert.ToInt32(reader.GetInt64(1));
        }
        return result;
    }

    #endregion

    #region GroupBy + Sum

    [Benchmark]
    [BenchmarkCategory("GroupBy_Sum")]
    public Dictionary<string, decimal> List_GroupBy_Sum()
    {
        return _list
            .GroupBy(x => x.Category)
            .ToDictionary(g => g.Key, g => g.Sum(x => x.Salary));
    }

    [Benchmark]
    [BenchmarkCategory("GroupBy_Sum")]
    public Dictionary<string, decimal> FrozenArrow_GroupBy_Sum()
    {
        return _frozenArrow.AsQueryable()
            .GroupBy(x => x.Category)
            .ToDictionary(g => g.Key, g => g.Sum(x => x.Salary));
    }

    [Benchmark]
    [BenchmarkCategory("GroupBy_Sum")]
    public Dictionary<string, decimal> DuckDB_GroupBy_Sum()
    {
        var result = new Dictionary<string, decimal>();
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT Category, SUM(Salary) FROM items GROUP BY Category";
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            result[reader.GetString(0)] = reader.GetDecimal(1);
        }
        return result;
    }

    #endregion

    #region GroupBy + Average

    [Benchmark]
    [BenchmarkCategory("GroupBy_Average")]
    public Dictionary<string, double> List_GroupBy_Average()
    {
        return _list
            .GroupBy(x => x.Department)
            .ToDictionary(g => g.Key, g => g.Average(x => x.PerformanceScore));
    }

    [Benchmark]
    [BenchmarkCategory("GroupBy_Average")]
    public Dictionary<string, double> FrozenArrow_GroupBy_Average()
    {
        return _frozenArrow.AsQueryable()
            .GroupBy(x => x.Department)
            .ToDictionary(g => g.Key, g => g.Average(x => x.PerformanceScore));
    }

    [Benchmark]
    [BenchmarkCategory("GroupBy_Average")]
    public Dictionary<string, double> DuckDB_GroupBy_Average()
    {
        var result = new Dictionary<string, double>();
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT Department, AVG(PerformanceScore) FROM items GROUP BY Department";
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            result[reader.GetString(0)] = reader.GetDouble(1);
        }
        return result;
    }

    #endregion

    #region GroupBy + Multiple Aggregates

    [Benchmark]
    [BenchmarkCategory("GroupBy_MultiAggregate")]
    public int List_GroupBy_MultiAggregate()
    {
        return _list
            .GroupBy(x => x.Category)
            .Select(g => new 
            { 
                Category = g.Key, 
                Total = g.Sum(x => x.Salary),
                Count = g.Count(),
                AvgAge = g.Average(x => x.Age)
            })
            .ToList()
            .Count;
    }

    [Benchmark]
    [BenchmarkCategory("GroupBy_MultiAggregate")]
    public int FrozenArrow_GroupBy_MultiAggregate()
    {
        return _frozenArrow.AsQueryable()
            .GroupBy(x => x.Category)
            .Select(g => new 
            { 
                Category = g.Key, 
                Total = g.Sum(x => x.Salary),
                Count = g.Count(),
                AvgAge = g.Average(x => x.Age)
            })
            .ToList()
            .Count;
    }

    [Benchmark]
    [BenchmarkCategory("GroupBy_MultiAggregate")]
    public int DuckDB_GroupBy_MultiAggregate()
    {
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT Category, SUM(Salary), COUNT(*), AVG(Age) FROM items GROUP BY Category";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    #endregion

    #region GroupBy + Filter + Sum

    [Benchmark]
    [BenchmarkCategory("GroupBy_WithFilter")]
    public int List_GroupBy_WithFilter_Sum()
    {
        return _list
            .Where(x => x.IsActive)
            .GroupBy(x => x.Category)
            .Select(g => new { Category = g.Key, Total = g.Sum(x => x.Salary) })
            .ToList()
            .Count;
    }

    [Benchmark]
    [BenchmarkCategory("GroupBy_WithFilter")]
    public int FrozenArrow_GroupBy_WithFilter_Sum()
    {
        return _frozenArrow.AsQueryable()
            .Where(x => x.IsActive)
            .GroupBy(x => x.Category)
            .Select(g => new { Category = g.Key, Total = g.Sum(x => x.Salary) })
            .ToList()
            .Count;
    }

    [Benchmark]
    [BenchmarkCategory("GroupBy_WithFilter")]
    public int DuckDB_GroupBy_WithFilter_Sum()
    {
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT Category, SUM(Salary) FROM items WHERE IsActive = true GROUP BY Category";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    #endregion

    #region GroupBy + Min/Max

    [Benchmark]
    [BenchmarkCategory("GroupBy_MinMax")]
    public int List_GroupBy_MinMax()
    {
        return _list
            .GroupBy(x => x.Department)
            .Select(g => new 
            { 
                Department = g.Key, 
                MinAge = g.Min(x => x.Age),
                MaxAge = g.Max(x => x.Age)
            })
            .ToList()
            .Count;
    }

    [Benchmark]
    [BenchmarkCategory("GroupBy_MinMax")]
    public int FrozenArrow_GroupBy_MinMax()
    {
        return _frozenArrow.AsQueryable()
            .GroupBy(x => x.Department)
            .Select(g => new 
            { 
                Department = g.Key, 
                MinAge = g.Min(x => x.Age),
                MaxAge = g.Max(x => x.Age)
            })
            .ToList()
            .Count;
    }

    [Benchmark]
    [BenchmarkCategory("GroupBy_MinMax")]
    public int DuckDB_GroupBy_MinMax()
    {
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT Department, MIN(Age), MAX(Age) FROM items GROUP BY Department";
        using var reader = cmd.ExecuteReader();
        int count = 0;
        while (reader.Read()) count++;
        return count;
    }

    #endregion
}
