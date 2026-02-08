using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using DuckDB.NET.Data;
using FrozenArrow.Query;

namespace FrozenArrow.Benchmarks;

/// <summary>
/// Benchmarks for recently added advanced features:
/// - DateTime predicates
/// - Boolean predicates
/// - NULL filtering
/// - DISTINCT operation
/// - ORDER BY operation
/// - Complex OR expressions
/// </summary>
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[GroupBenchmarksBy(BenchmarkDotNet.Configs.BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
[ShortRunJob]
public class AdvancedFeatureBenchmarks
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

    #region DateTime Range Query

    [Benchmark]
    [BenchmarkCategory("DateTime")]
    public int List_DateTime_Range()
    {
        var startDate = new DateTime(2020, 6, 1);
        var endDate = new DateTime(2021, 6, 1);
        return _list.Where(x => x.HireDate >= startDate && x.HireDate < endDate).Count();
    }

    [Benchmark]
    [BenchmarkCategory("DateTime")]
    public int FrozenArrow_DateTime_Range()
    {
        var startDate = new DateTime(2020, 6, 1);
        var endDate = new DateTime(2021, 6, 1);
        return _frozenArrow.AsQueryable()
            .Where(x => x.HireDate >= startDate && x.HireDate < endDate)
            .Count();
    }

    [Benchmark]
    [BenchmarkCategory("DateTime")]
    public int DuckDB_DateTime_Range()
    {
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM items WHERE HireDate >= '2020-06-01' AND HireDate < '2021-06-01'";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    #endregion

    #region Boolean Predicate

    [Benchmark]
    [BenchmarkCategory("Boolean")]
    public int List_Boolean_Filter()
    {
        return _list.Where(x => x.IsActive).Count();
    }

    [Benchmark]
    [BenchmarkCategory("Boolean")]
    public int FrozenArrow_Boolean_Filter()
    {
        return _frozenArrow.AsQueryable().Where(x => x.IsActive).Count();
    }

    [Benchmark]
    [BenchmarkCategory("Boolean")]
    public int DuckDB_Boolean_Filter()
    {
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM items WHERE IsActive = true";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    #endregion

    #region DISTINCT Operation

    [Benchmark]
    [BenchmarkCategory("Distinct")]
    public int List_Distinct_Categories()
    {
        return _list.Select(x => x.Category).Distinct().Count();
    }

    [Benchmark]
    [BenchmarkCategory("Distinct")]
    public int FrozenArrow_Distinct_Categories()
    {
        return _frozenArrow.AsQueryable().Select(x => x.Category).Distinct().Count();
    }

    [Benchmark]
    [BenchmarkCategory("Distinct")]
    public int DuckDB_Distinct_Categories()
    {
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(DISTINCT Category) FROM items";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    #endregion

    #region ORDER BY with LIMIT

    [Benchmark]
    [BenchmarkCategory("OrderBy")]
    public List<decimal> List_OrderBy_Top10()
    {
        return _list.OrderByDescending(x => x.Salary).Take(10).Select(x => x.Salary).ToList();
    }

    [Benchmark]
    [BenchmarkCategory("OrderBy")]
    public List<decimal> FrozenArrow_OrderBy_Top10()
    {
        return _frozenArrow.AsQueryable()
            .OrderByDescending(x => x.Salary)
            .Take(10)
            .Select(x => x.Salary)
            .ToList();
    }

    [Benchmark]
    [BenchmarkCategory("OrderBy")]
    public List<decimal> DuckDB_OrderBy_Top10()
    {
        var results = new List<decimal>();
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT Salary FROM items ORDER BY Salary DESC LIMIT 10";
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            results.Add(reader.GetDecimal(0));
        }
        return results;
    }

    #endregion

    #region Complex OR Expression

    [Benchmark]
    [BenchmarkCategory("ComplexOr")]
    public int List_Complex_Or()
    {
        return _list.Where(x =>
            (x.Age < 25 && x.Category == "Engineering") ||
            (x.Age > 55 && x.Category == "Executive") ||
            (x.Salary > 100000 && x.IsActive)
        ).Count();
    }

    [Benchmark]
    [BenchmarkCategory("ComplexOr")]
    public int FrozenArrow_Complex_Or()
    {
        return _frozenArrow.AsQueryable().Where(x =>
            (x.Age < 25 && x.Category == "Engineering") ||
            (x.Age > 55 && x.Category == "Executive") ||
            (x.Salary > 100000 && x.IsActive)
        ).Count();
    }

    [Benchmark]
    [BenchmarkCategory("ComplexOr")]
    public int DuckDB_Complex_Or()
    {
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = @"
            SELECT COUNT(*) FROM items 
            WHERE (Age < 25 AND Category = 'Engineering')
               OR (Age > 55 AND Category = 'Executive')
               OR (Salary > 100000 AND IsActive = true)
        ";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    #endregion

    #region Multi-Column Sort

    [Benchmark]
    [BenchmarkCategory("MultiSort")]
    public List<QueryBenchmarkItem> List_MultiColumn_Sort()
    {
        return _list
            .OrderBy(x => x.Category)
            .ThenByDescending(x => x.Salary)
            .Take(100)
            .ToList();
    }

    [Benchmark]
    [BenchmarkCategory("MultiSort")]
    public List<QueryBenchmarkItem> FrozenArrow_MultiColumn_Sort()
    {
        return _frozenArrow.AsQueryable()
            .OrderBy(x => x.Category)
            .ThenByDescending(x => x.Salary)
            .Take(100)
            .ToList();
    }

    [Benchmark]
    [BenchmarkCategory("MultiSort")]
    public List<QueryBenchmarkItem> DuckDB_MultiColumn_Sort()
    {
        var results = new List<QueryBenchmarkItem>();
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT * FROM items ORDER BY Category ASC, Salary DESC LIMIT 100";
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
        return results;
    }

    #endregion

    #region String LIKE Pattern

    [Benchmark]
    [BenchmarkCategory("StringLike")]
    public int List_String_StartsWith()
    {
        return _list.Where(x => x.Name.StartsWith("Person_1")).Count();
    }

    [Benchmark]
    [BenchmarkCategory("StringLike")]
    public int FrozenArrow_String_StartsWith()
    {
        return _frozenArrow.AsQueryable().Where(x => x.Name.StartsWith("Person_1")).Count();
    }

    [Benchmark]
    [BenchmarkCategory("StringLike")]
    public int DuckDB_String_Like()
    {
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM items WHERE Name LIKE 'Person_1%'";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    #endregion

    #region Aggregation with DISTINCT

    [Benchmark]
    [BenchmarkCategory("DistinctAgg")]
    public decimal List_Sum_Distinct()
    {
        return _list.Select(x => x.Salary).Distinct().Sum();
    }

    [Benchmark]
    [BenchmarkCategory("DistinctAgg")]
    public decimal FrozenArrow_Sum_Distinct()
    {
        return _frozenArrow.AsQueryable().Select(x => x.Salary).Distinct().Sum();
    }

    [Benchmark]
    [BenchmarkCategory("DistinctAgg")]
    public decimal DuckDB_Sum_Distinct()
    {
        using var cmd = _duckDbConnection.CreateCommand();
        cmd.CommandText = "SELECT SUM(DISTINCT Salary) FROM items";
        return Convert.ToDecimal(cmd.ExecuteScalar());
    }

    #endregion
}
