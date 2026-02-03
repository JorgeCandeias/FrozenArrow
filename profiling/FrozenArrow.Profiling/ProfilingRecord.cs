namespace FrozenArrow.Profiling;

/// <summary>
/// Test data model for profiling scenarios.
/// Uses primitive types to ensure compatibility with all query paths.
/// </summary>
[ArrowRecord]
public sealed partial class ProfilingRecord
{
    [ArrowArray] public int Id { get; set; }
    [ArrowArray] public int Age { get; set; }
    [ArrowArray] public int DepartmentId { get; set; }
    [ArrowArray] public double Salary { get; set; }
    [ArrowArray] public double PerformanceScore { get; set; }
    [ArrowArray] public bool IsActive { get; set; }
    [ArrowArray] public bool IsManager { get; set; }
    [ArrowArray] public long TenureDays { get; set; }
}

/// <summary>
/// Factory for generating test data.
/// </summary>
public static class ProfilingDataFactory
{
    public static List<ProfilingRecord> Generate(int count, int? seed = null)
    {
        var random = seed.HasValue ? new Random(seed.Value) : new Random(42);
        var records = new List<ProfilingRecord>(count);

        for (int i = 0; i < count; i++)
        {
            records.Add(new ProfilingRecord
            {
                Id = i,
                Age = 20 + random.Next(45), // 20-64
                DepartmentId = random.Next(20), // 20 departments
                Salary = 30000.0 + random.NextDouble() * 170000.0, // 30k-200k
                PerformanceScore = random.NextDouble() * 5.0, // 0-5
                IsActive = random.NextDouble() > 0.3, // 70% active
                IsManager = random.NextDouble() > 0.85, // 15% managers
                TenureDays = random.Next(3650) // 0-10 years
            });
        }

        return records;
    }
}
