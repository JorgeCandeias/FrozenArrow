using FrozenArrow.Query;

namespace FrozenArrow.Profiling.Scenarios;

/// <summary>
/// Profiling scenario for nullable column operations.
/// Tests the performance of null filtering in various data distributions.
/// </summary>
public class NullableColumnScenario : BaseScenario
{
    private FrozenArrow<NullableRecord> _nullableData = null!;
    protected int RowCount { get; private set; }

    public override string Name => "NullableColumns";
    public override string Description => "Null filtering performance with various null distributions";

    public override void Setup(ProfilingConfig config)
    {
        base.Setup(config);
        RowCount = config.RowCount;
        
        // Generate data with nullable columns
        var records = GenerateNullableData(config.RowCount);
        _nullableData = records.ToFrozenArrow();
    }

    public override object? RunIteration()
    {
        // Scenario 1: Filter on nullable bool (70% non-null)
        StartPhase("Bool Filter (70% non-null)");
        var boolFilterResults = _nullableData.AsQueryable()
            .Where(x => x.IsActive!.Value)  // Use .Value for nullable bool
            .Count();
        EndPhase("Bool Filter (70% non-null)");

        // Scenario 2: Filter on nullable int (50% non-null)
        StartPhase("Int Filter (50% non-null)");
        var intFilterResults = _nullableData.AsQueryable()
            .Where(x => x.Age > 30)
            .Count();
        EndPhase("Int Filter (50% non-null)");

        // Scenario 3: Filter on nullable double (80% non-null)
        StartPhase("Double Filter (80% non-null)");
        var doubleFilterResults = _nullableData.AsQueryable()
            .Where(x => x.Salary > 50000.0)
            .Count();
        EndPhase("Double Filter (80% non-null)");

        // Scenario 4: Multi-filter on nullable columns
        StartPhase("Multi-Filter Nullable");
        var multiFilterResults = _nullableData.AsQueryable()
            .Where(x => x.Age > 25 && x.Salary > 40000.0)
            .Count();
        EndPhase("Multi-Filter Nullable");

        // Scenario 5: Aggregation on nullable columns
        StartPhase("Sum Nullable");
        var sumResults = _nullableData.AsQueryable()
            .Where(x => x.Salary > 30000.0)
            .Sum(x => x.Salary);
        EndPhase("Sum Nullable");

        // Scenario 6: High null percentage (10% non-null)
        StartPhase("High Null Percentage (10% non-null)");
        var highNullResults = _nullableData.AsQueryable()
            .Where(x => x.OptionalScore > 0.5)
            .Count();
        EndPhase("High Null Percentage (10% non-null)");

        // Scenario 7: Materialization with nullable columns
        StartPhase("Materialization Nullable");
        var materializeResults = _nullableData.AsQueryable()
            .Where(x => x.Age > 30)
            .Take(1000)
            .ToList();
        EndPhase("Materialization Nullable");

        // Sanity checks
        if (boolFilterResults < 0 || boolFilterResults > RowCount)
            throw new InvalidOperationException($"Bool filter returned invalid count: {boolFilterResults}");
        if (sumResults < 0)
            throw new InvalidOperationException($"Sum returned negative value: {sumResults}");

        return new
        {
            BoolFilter = boolFilterResults,
            IntFilter = intFilterResults,
            DoubleFilter = doubleFilterResults,
            MultiFilter = multiFilterResults,
            SumResult = sumResults,
            HighNullFilter = highNullResults,
            MaterializedCount = materializeResults.Count
        };
    }

    private static List<NullableRecord> GenerateNullableData(int count)
    {
        var random = new Random(42);
        var records = new List<NullableRecord>(count);

        for (int i = 0; i < count; i++)
        {
            records.Add(new NullableRecord
            {
                Id = i,
                // IsActive: 70% non-null, 30% null
                IsActive = random.Next(100) < 70 ? random.Next(2) == 1 : null,
                // Age: 50% non-null, 50% null
                Age = random.Next(100) < 50 ? random.Next(18, 80) : null,
                // Salary: 80% non-null, 20% null
                Salary = random.Next(100) < 80 ? 30000.0 + random.NextDouble() * 170000.0 : null,
                // OptionalScore: 10% non-null, 90% null (high null percentage)
                OptionalScore = random.Next(100) < 10 ? random.NextDouble() : null
            });
        }

        return records;
    }

    public override void Cleanup()
    {
        _nullableData?.Dispose();
        base.Cleanup();
    }
}

/// <summary>
/// Test record with nullable columns for profiling.
/// </summary>
[ArrowRecord]
public sealed partial class NullableRecord
{
    [ArrowArray] public int Id { get; set; }
    [ArrowArray] public bool? IsActive { get; set; }
    [ArrowArray] public int? Age { get; set; }
    [ArrowArray] public double? Salary { get; set; }
    [ArrowArray] public double? OptionalScore { get; set; }
}
