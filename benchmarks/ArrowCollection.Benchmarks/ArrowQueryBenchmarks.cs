using ArrowCollection.Query;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;

namespace ArrowCollection.Benchmarks;

/// <summary>
/// Benchmarks comparing query performance across different approaches:
/// 1. List&lt;T&gt; with LINQ-to-Objects (baseline)
/// 2. ArrowCollection with Enumerable LINQ (materializes all items then filters)
/// 3. ArrowCollection with ArrowQuery (column-level filtering, selective materialization)
/// </summary>
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[GroupBenchmarksBy(BenchmarkDotNet.Configs.BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
public class ArrowQueryBenchmarks
{
    private List<QueryBenchmarkItem> _list = null!;
    private ArrowCollection<QueryBenchmarkItem> _arrowCollection = null!;

    [Params(10_000, 100_000)]
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

    #region High Selectivity (~5% match) - ArrowQuery should dominate

    /// <summary>
    /// Baseline: List with LINQ-to-Objects, highly selective filter.
    /// Filter: Age > 55 (~5% of items match)
    /// </summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("HighSelectivity")]
    public int List_HighSelectivity_ToList()
    {
        return _list.Where(x => x.Age > 55).ToList().Count;
    }

    /// <summary>
    /// ArrowCollection with Enumerable.Where - materializes ALL items then filters.
    /// This is the "wrong" way to query ArrowCollection.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("HighSelectivity")]
    public int ArrowCollection_Enumerable_HighSelectivity_ToList()
    {
        return _arrowCollection.Where(x => x.Age > 55).ToList().Count;
    }

    /// <summary>
    /// ArrowQuery with column-level filtering - only materializes matching rows.
    /// This is the "right" way to query ArrowCollection.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("HighSelectivity")]
    public int ArrowQuery_HighSelectivity_ToList()
    {
        return _arrowCollection.AsQueryable().Where(x => x.Age > 55).ToList().Count;
    }

    #endregion

    #region Medium Selectivity (~30% match)

    /// <summary>
    /// Baseline: List with LINQ-to-Objects, medium selective filter.
    /// Filter: Age > 40 (~30% of items match)
    /// </summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("MediumSelectivity")]
    public int List_MediumSelectivity_ToList()
    {
        return _list.Where(x => x.Age > 40).ToList().Count;
    }

    [Benchmark]
    [BenchmarkCategory("MediumSelectivity")]
    public int ArrowCollection_Enumerable_MediumSelectivity_ToList()
    {
        return _arrowCollection.Where(x => x.Age > 40).ToList().Count;
    }

    [Benchmark]
    [BenchmarkCategory("MediumSelectivity")]
    public int ArrowQuery_MediumSelectivity_ToList()
    {
        return _arrowCollection.AsQueryable().Where(x => x.Age > 40).ToList().Count;
    }

    #endregion

    #region Low Selectivity (~90% match) - List should win

    /// <summary>
    /// Baseline: List with LINQ-to-Objects, low selective filter (most items match).
    /// Filter: Age > 20 (~90% of items match)
    /// </summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("LowSelectivity")]
    public int List_LowSelectivity_ToList()
    {
        return _list.Where(x => x.Age > 20).ToList().Count;
    }

    [Benchmark]
    [BenchmarkCategory("LowSelectivity")]
    public int ArrowCollection_Enumerable_LowSelectivity_ToList()
    {
        return _arrowCollection.Where(x => x.Age > 20).ToList().Count;
    }

    [Benchmark]
    [BenchmarkCategory("LowSelectivity")]
    public int ArrowQuery_LowSelectivity_ToList()
    {
        return _arrowCollection.AsQueryable().Where(x => x.Age > 20).ToList().Count;
    }

    #endregion

    #region Count (no materialization) - ArrowQuery should dominate

    /// <summary>
    /// Count with List - must iterate all matching items.
    /// </summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Count")]
    public int List_Count()
    {
        return _list.Where(x => x.Age > 40).Count();
    }

    /// <summary>
    /// Count with ArrowCollection Enumerable - materializes all items, then counts.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Count")]
    public int ArrowCollection_Enumerable_Count()
    {
        return _arrowCollection.Where(x => x.Age > 40).Count();
    }

    /// <summary>
    /// Count with ArrowQuery - counts selection bitmap, NO materialization!
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Count")]
    public int ArrowQuery_Count()
    {
        return _arrowCollection.AsQueryable().Where(x => x.Age > 40).Count();
    }

    #endregion

    #region Any (short-circuit) - ArrowQuery should win for low-cardinality matches

    /// <summary>
    /// Any with List - short-circuits on first match.
    /// </summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Any")]
    public bool List_Any()
    {
        return _list.Where(x => x.Category == "Executive").Any();
    }

    /// <summary>
    /// Any with ArrowCollection Enumerable - materializes until first match.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Any")]
    public bool ArrowCollection_Enumerable_Any()
    {
        return _arrowCollection.Where(x => x.Category == "Executive").Any();
    }

    /// <summary>
    /// Any with ArrowQuery - scans column until first match, no materialization.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Any")]
    public bool ArrowQuery_Any()
    {
        return _arrowCollection.AsQueryable().Where(x => x.Category == "Executive").Any();
    }

    #endregion

    #region First (early termination)

    /// <summary>
    /// First with List - stops at first match.
    /// </summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("First")]
    public string List_First()
    {
        return _list.Where(x => x.Age > 50).First().Name;
    }

    /// <summary>
    /// First with ArrowCollection Enumerable - materializes until first match.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("First")]
    public string ArrowCollection_Enumerable_First()
    {
        return _arrowCollection.Where(x => x.Age > 50).First().Name;
    }

    /// <summary>
    /// First with ArrowQuery - scans column, materializes only first match.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("First")]
    public string ArrowQuery_First()
    {
        return _arrowCollection.AsQueryable().Where(x => x.Age > 50).First().Name;
    }

    #endregion

    #region Multiple Predicates (AND)

    /// <summary>
    /// Multiple predicates with List.
    /// Filter: Age > 30 AND IsActive AND Category == "Engineering"
    /// </summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("MultiPredicate")]
    public int List_MultiPredicate_ToList()
    {
        return _list
            .Where(x => x.Age > 30 && x.IsActive && x.Category == "Engineering")
            .ToList().Count;
    }

    [Benchmark]
    [BenchmarkCategory("MultiPredicate")]
    public int ArrowCollection_Enumerable_MultiPredicate_ToList()
    {
        return _arrowCollection
            .Where(x => x.Age > 30 && x.IsActive && x.Category == "Engineering")
            .ToList().Count;
    }

    [Benchmark]
    [BenchmarkCategory("MultiPredicate")]
    public int ArrowQuery_MultiPredicate_ToList()
    {
        return _arrowCollection
            .AsQueryable()
            .Where(x => x.Age > 30 && x.IsActive && x.Category == "Engineering")
            .ToList().Count;
    }

    #endregion

    #region String Contains

    /// <summary>
    /// String Contains with List.
    /// </summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("StringContains")]
    public int List_StringContains_ToList()
    {
        return _list.Where(x => x.Name.Contains("42")).ToList().Count;
    }

    [Benchmark]
    [BenchmarkCategory("StringContains")]
    public int ArrowCollection_Enumerable_StringContains_ToList()
    {
        return _arrowCollection.Where(x => x.Name.Contains("42")).ToList().Count;
    }

    [Benchmark]
    [BenchmarkCategory("StringContains")]
    public int ArrowQuery_StringContains_ToList()
    {
        return _arrowCollection.AsQueryable().Where(x => x.Name.Contains("42")).ToList().Count;
    }

    #endregion

    #region String Equality (should benefit from dictionary encoding)

    /// <summary>
    /// String equality with List.
    /// </summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("StringEquality")]
    public int List_StringEquality_ToList()
    {
        return _list.Where(x => x.Category == "Engineering").ToList().Count;
    }

    [Benchmark]
    [BenchmarkCategory("StringEquality")]
    public int ArrowCollection_Enumerable_StringEquality_ToList()
    {
        return _arrowCollection.Where(x => x.Category == "Engineering").ToList().Count;
    }

    [Benchmark]
    [BenchmarkCategory("StringEquality")]
    public int ArrowQuery_StringEquality_ToList()
    {
        return _arrowCollection.AsQueryable().Where(x => x.Category == "Engineering").ToList().Count;
    }

    #endregion

    #region Decimal Comparison

    /// <summary>
    /// Decimal comparison with List.
    /// Filter: Salary > 75000 (~40% match)
    /// </summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("DecimalComparison")]
    public int List_DecimalComparison_ToList()
    {
        return _list.Where(x => x.Salary > 75000m).ToList().Count;
    }

    [Benchmark]
    [BenchmarkCategory("DecimalComparison")]
    public int ArrowCollection_Enumerable_DecimalComparison_ToList()
    {
        return _arrowCollection.Where(x => x.Salary > 75000m).ToList().Count;
    }

    [Benchmark]
    [BenchmarkCategory("DecimalComparison")]
    public int ArrowQuery_DecimalComparison_ToList()
    {
        return _arrowCollection.AsQueryable().Where(x => x.Salary > 75000m).ToList().Count;
    }

    #endregion

    #region Column-Level Aggregates (Phase 1)

    /// <summary>
    /// Sum with List - iterates all items.
    /// </summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Sum")]
    public decimal List_Sum()
    {
        return _list.Where(x => x.IsActive).Sum(x => x.Salary);
    }

    /// <summary>
    /// Sum with ArrowCollection Enumerable - materializes all items, then sums.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Sum")]
    public decimal ArrowCollection_Enumerable_Sum()
    {
        return _arrowCollection.Where(x => x.IsActive).Sum(x => x.Salary);
    }

    /// <summary>
    /// Sum with ArrowQuery - column-level aggregation, NO materialization!
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Sum")]
    public decimal ArrowQuery_Sum()
    {
        return _arrowCollection.AsQueryable().Where(x => x.IsActive).Sum(x => x.Salary);
    }

    /// <summary>
    /// Average with List.
    /// </summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Average")]
    public double List_Average()
    {
        return _list.Where(x => x.IsActive).Average(x => x.Age);
    }

    /// <summary>
    /// Average with ArrowCollection Enumerable.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Average")]
    public double ArrowCollection_Enumerable_Average()
    {
        return _arrowCollection.Where(x => x.IsActive).Average(x => x.Age);
    }

    /// <summary>
    /// Average with ArrowQuery - column-level aggregation.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Average")]
    public double ArrowQuery_Average()
    {
        return _arrowCollection.AsQueryable().Where(x => x.IsActive).Average(x => x.Age);
    }

    /// <summary>
    /// Min with List.
    /// </summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Min")]
    public decimal List_Min()
    {
        return _list.Where(x => x.Category == "Engineering").Min(x => x.Salary);
    }

    /// <summary>
    /// Min with ArrowCollection Enumerable.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Min")]
    public decimal ArrowCollection_Enumerable_Min()
    {
        return _arrowCollection.Where(x => x.Category == "Engineering").Min(x => x.Salary);
    }

    /// <summary>
    /// Min with ArrowQuery - column-level aggregation.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Min")]
    public decimal ArrowQuery_Min()
    {
        return _arrowCollection.AsQueryable().Where(x => x.Category == "Engineering").Min(x => x.Salary);
    }

    /// <summary>
    /// Max with List.
    /// </summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Max")]
    public decimal List_Max()
    {
        return _list.Where(x => x.Category == "Engineering").Max(x => x.Salary);
    }

    /// <summary>
    /// Max with ArrowCollection Enumerable.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Max")]
    public decimal ArrowCollection_Enumerable_Max()
    {
        return _arrowCollection.Where(x => x.Category == "Engineering").Max(x => x.Salary);
    }

    /// <summary>
    /// Max with ArrowQuery - column-level aggregation.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Max")]
    public decimal ArrowQuery_Max()
    {
        return _arrowCollection.AsQueryable().Where(x => x.Category == "Engineering").Max(x => x.Salary);
    }

    #endregion
}

/// <summary>
/// Benchmark item with realistic data distribution.
/// </summary>
[ArrowRecord]
public class QueryBenchmarkItem
{
    [ArrowArray] public int Id { get; set; }
    [ArrowArray] public string Name { get; set; } = string.Empty;
    [ArrowArray] public int Age { get; set; }
    [ArrowArray] public decimal Salary { get; set; }
    [ArrowArray] public bool IsActive { get; set; }
    [ArrowArray] public string Category { get; set; } = string.Empty;
    [ArrowArray] public string Department { get; set; } = string.Empty;
    [ArrowArray] public DateTime HireDate { get; set; }
    [ArrowArray] public double PerformanceScore { get; set; }
    [ArrowArray] public string Region { get; set; } = string.Empty;
}

/// <summary>
/// Factory to generate benchmark items with realistic data distribution.
/// </summary>
public static class QueryBenchmarkItemFactory
{
    private static readonly string[] Categories = 
        ["Engineering", "Marketing", "Sales", "HR", "Finance", "Operations", "Executive", "Support"];
    
    private static readonly string[] Departments =
        ["Dept_A", "Dept_B", "Dept_C", "Dept_D", "Dept_E"];
    
    private static readonly string[] Regions =
        ["North", "South", "East", "West", "Central"];

    public static List<QueryBenchmarkItem> Generate(int count, int seed = 42)
    {
        var random = new Random(seed);
        var items = new List<QueryBenchmarkItem>(count);
        var baseDate = new DateTime(2020, 1, 1);

        for (int i = 0; i < count; i++)
        {
            // Age: uniform distribution 20-60
            var age = 20 + random.Next(41);
            
            // IsActive: 70% active
            var isActive = random.NextDouble() < 0.7;
            
            // Salary: correlated with age, range 40K-150K
            var baseSalary = 40000 + (age - 20) * 1000;
            var salary = baseSalary + random.Next(-5000, 15000);
            
            // Category: weighted distribution (Engineering most common)
            var categoryIndex = random.NextDouble() switch
            {
                < 0.35 => 0, // Engineering 35%
                < 0.50 => 1, // Marketing 15%
                < 0.65 => 2, // Sales 15%
                < 0.75 => 3, // HR 10%
                < 0.85 => 4, // Finance 10%
                < 0.92 => 5, // Operations 7%
                < 0.97 => 6, // Executive 5%
                _ => 7       // Support 3%
            };

            items.Add(new QueryBenchmarkItem
            {
                Id = i,
                Name = $"Person_{i}",
                Age = age,
                Salary = salary,
                IsActive = isActive,
                Category = Categories[categoryIndex],
                Department = Departments[random.Next(Departments.Length)],
                HireDate = baseDate.AddDays(random.Next(0, 1500)),
                PerformanceScore = Math.Round(random.NextDouble() * 5, 2),
                Region = Regions[random.Next(Regions.Length)]
            });
        }

        return items;
    }
}
