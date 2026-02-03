namespace FrozenArrow.MemoryAnalysis.Shared;

/// <summary>
/// Standard model for memory analysis (7 columns).
/// </summary>
[ArrowRecord]
public class MemoryAnalysisItem
{
    [ArrowArray] public int Id { get; set; }
    [ArrowArray] public string Name { get; set; } = string.Empty;
    [ArrowArray] public int Age { get; set; }
    [ArrowArray] public decimal Salary { get; set; }
    [ArrowArray] public bool IsActive { get; set; }
    [ArrowArray] public string Category { get; set; } = string.Empty;
    [ArrowArray] public string Department { get; set; } = string.Empty;
}

/// <summary>
/// Factory to generate standard model items.
/// </summary>
public static class MemoryAnalysisItemFactory
{
    private static readonly string[] Categories = 
        ["Engineering", "Marketing", "Sales", "HR", "Finance", "Operations"];
    
    private static readonly string[] Departments =
        ["Dept_A", "Dept_B", "Dept_C", "Dept_D", "Dept_E"];

    public static List<MemoryAnalysisItem> Generate(int count, int seed = 42)
    {
        var random = new Random(seed);
        var items = new List<MemoryAnalysisItem>(count);

        for (int i = 0; i < count; i++)
        {
            var age = 20 + random.Next(41);
            items.Add(new MemoryAnalysisItem
            {
                Id = i,
                Name = $"Person_{i}",
                Age = age,
                Salary = 40000 + (age - 20) * 1000 + random.Next(-5000, 15000),
                IsActive = random.NextDouble() < 0.7,
                Category = Categories[random.Next(Categories.Length)],
                Department = Departments[random.Next(Departments.Length)]
            });
        }

        return items;
    }

    public static IEnumerable<MemoryAnalysisItem> GenerateEnumerable(int count, int seed = 42)
    {
        var random = new Random(seed);

        for (int i = 0; i < count; i++)
        {
            var age = 20 + random.Next(41);
            yield return new MemoryAnalysisItem
            {
                Id = i,
                Name = $"Person_{i}",
                Age = age,
                Salary = 40000 + (age - 20) * 1000 + random.Next(-5000, 15000),
                IsActive = random.NextDouble() < 0.7,
                Category = Categories[random.Next(Categories.Length)],
                Department = Departments[random.Next(Departments.Length)]
            };
        }
    }
}
