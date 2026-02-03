using FrozenArrow.Query;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using DuckDB.NET.Data;

namespace FrozenArrow.Benchmarks;

/// <summary>
/// Comprehensive benchmarks comparing FrozenArrow ArrowQuery performance against in-process DuckDB.
/// 
/// Covers all ArrowQuery supported operations:
/// - Where (various selectivities)
/// - Count, Any, All
/// - Sum, Average, Min, Max
/// - GroupBy with aggregates
/// - First, Take, Skip
/// - ToList materialization
/// </summary>
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[GroupBenchmarksBy(BenchmarkDotNet.Configs.BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
[ShortRunJob]
public class DuckDbComparisonBenchmarks
{
    private List<QueryBenchmarkItem> _list = null!;
    private FrozenArrow<QueryBenchmarkItem> _frozenArrow = null!;
    private DuckDBConnection _duckDbConnection = null!;

    [Params(100_000, 1_000_000)]
    public int ItemCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _list = QueryBenchmarkItemFactory.Generate(ItemCount);
        _frozenArrow = _list.ToFrozenArrow();

        // Setup in-memory DuckDB
        _duckDbConnection = new DuckDBConnection("DataSource=:memory:");
        _duckDbConnection.Open();

        // Create table
        using var createCmd = _duckDbConnection.CreateCommand();
        createCmd.CommandText = """
            CREATE TABLE benchmark_items (
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

        // Use Appender for fast bulk insert
        using var appender = _duckDbConnection.CreateAppender("benchmark_items");
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

    #region Where + Count (High Selectivity ~5%)

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Count_HighSelectivity")]
    public int List_Count_HighSelectivity()
    {
        return _list.Where(x => x.Age > 55).Count();
    }

    [Benchmark]
    [BenchmarkCategory("Count_HighSelectivity")]
    public int FrozenArrow_Count_HighSelectivity()
    {
        return _frozenArrow.AsQueryable().Where(x => x.Age > 55).Count();
    }

    [Benchmark]
    [BenchmarkCategory("Count_HighSelectivity")]
    public int DuckDb_Count_HighSelectivity()
    {
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM benchmark_items WHERE Age > 55";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    #endregion

    #region Where + Count (Low Selectivity ~70%)

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Count_LowSelectivity")]
    public int List_Count_LowSelectivity()
    {
        return _list.Where(x => x.IsActive).Count();
    }

    [Benchmark]
    [BenchmarkCategory("Count_LowSelectivity")]
    public int FrozenArrow_Count_LowSelectivity()
    {
        return _frozenArrow.AsQueryable().Where(x => x.IsActive).Count();
    }

    [Benchmark]
    [BenchmarkCategory("Count_LowSelectivity")]
    public int DuckDb_Count_LowSelectivity()
    {
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM benchmark_items WHERE IsActive = true";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    #endregion

    #region Any (Short-circuit evaluation)

    [Benchmark(Baseline = true)]
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
    public bool DuckDb_Any()
    {
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT EXISTS(SELECT 1 FROM benchmark_items WHERE Age > 55 AND Category = 'Executive')";
        return Convert.ToBoolean(cmd.ExecuteScalar());
    }

    #endregion

    #region First (Stop at first match)

    [Benchmark(Baseline = true)]
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
    public int DuckDb_First()
    {
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT Id FROM benchmark_items WHERE Age > 55 LIMIT 1";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    #endregion

    #region Sum Aggregation

    [Benchmark(Baseline = true)]
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
    public decimal DuckDb_Sum()
    {
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT SUM(Salary) FROM benchmark_items WHERE IsActive = true";
        return Convert.ToDecimal(cmd.ExecuteScalar());
    }

    #endregion

    #region Average Aggregation

    [Benchmark(Baseline = true)]
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
    public double DuckDb_Average()
    {
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT AVG(PerformanceScore) FROM benchmark_items WHERE Age > 30";
        return Convert.ToDouble(cmd.ExecuteScalar());
    }

    #endregion

    #region Min Aggregation

    [Benchmark(Baseline = true)]
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
    public decimal DuckDb_Min()
    {
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT MIN(Salary) FROM benchmark_items WHERE Age > 40";
        return Convert.ToDecimal(cmd.ExecuteScalar());
    }

    #endregion

    #region Max Aggregation

    [Benchmark(Baseline = true)]
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
    public decimal DuckDb_Max()
    {
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT MAX(Salary) FROM benchmark_items WHERE Age > 40";
        return Convert.ToDecimal(cmd.ExecuteScalar());
    }

    #endregion

    #region GroupBy + Count

    [Benchmark(Baseline = true)]
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
    public Dictionary<string, int> DuckDb_GroupBy_Count()
    {
        var result = new Dictionary<string, int>();
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT Category, COUNT(*) FROM benchmark_items GROUP BY Category";
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            result[reader.GetString(0)] = Convert.ToInt32(reader.GetInt64(1));
        }
        return result;
    }

    #endregion

    #region GroupBy + Sum

    [Benchmark(Baseline = true)]
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
    public Dictionary<string, decimal> DuckDb_GroupBy_Sum()
    {
        var result = new Dictionary<string, decimal>();
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT Category, SUM(Salary) FROM benchmark_items GROUP BY Category";
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            result[reader.GetString(0)] = reader.GetDecimal(1);
        }
        return result;
    }

    #endregion

    #region GroupBy + Average

    [Benchmark(Baseline = true)]
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
    public Dictionary<string, double> DuckDb_GroupBy_Average()
    {
        var result = new Dictionary<string, double>();
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT Department, AVG(PerformanceScore) FROM benchmark_items GROUP BY Department";
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            result[reader.GetString(0)] = reader.GetDouble(1);
        }
        return result;
    }

    #endregion

    #region Take (Pagination)

    [Benchmark(Baseline = true)]
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
    public int DuckDb_Take()
    {
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM (SELECT 1 FROM benchmark_items WHERE IsActive = true LIMIT 100)";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    #endregion

    #region Skip + Take (Pagination)

    [Benchmark(Baseline = true)]
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
    public int DuckDb_SkipTake()
    {
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM (SELECT 1 FROM benchmark_items WHERE IsActive = true LIMIT 100 OFFSET 1000)";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    #endregion

    #region Compound Filter (Multiple AND conditions)

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("CompoundFilter")]
    public int List_CompoundFilter()
    {
        return _list.Where(x => x.Age > 30 && x.IsActive && x.Category == "Engineering").Count();
    }

    [Benchmark]
    [BenchmarkCategory("CompoundFilter")]
    public int FrozenArrow_CompoundFilter()
    {
        return _frozenArrow.AsQueryable()
            .Where(x => x.Age > 30 && x.IsActive && x.Category == "Engineering")
            .Count();
    }

    [Benchmark]
    [BenchmarkCategory("CompoundFilter")]
    public int DuckDb_CompoundFilter()
    {
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM benchmark_items WHERE Age > 30 AND IsActive = true AND Category = 'Engineering'";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    #endregion

    #region String Equality Filter

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("StringEquality")]
    public int List_StringEquality()
    {
        return _list.Where(x => x.Category == "Engineering").Count();
    }

    [Benchmark]
    [BenchmarkCategory("StringEquality")]
    public int FrozenArrow_StringEquality()
    {
        return _frozenArrow.AsQueryable().Where(x => x.Category == "Engineering").Count();
    }

    [Benchmark]
    [BenchmarkCategory("StringEquality")]
    public int DuckDb_StringEquality()
    {
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM benchmark_items WHERE Category = 'Engineering'";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    #endregion

    #region Filter + ToList (Full Materialization)

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("ToList")]
    public int List_ToList()
    {
        return _list.Where(x => x.Age > 55).ToList().Count;
    }

    [Benchmark]
    [BenchmarkCategory("ToList")]
    public int FrozenArrow_ToList()
    {
        return _frozenArrow.AsQueryable().Where(x => x.Age > 55).ToList().Count;
    }

    [Benchmark]
    [BenchmarkCategory("ToList")]
    public int DuckDb_ToList()
    {
        var results = new List<QueryBenchmarkItem>();
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT * FROM benchmark_items WHERE Age > 55";
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
}

