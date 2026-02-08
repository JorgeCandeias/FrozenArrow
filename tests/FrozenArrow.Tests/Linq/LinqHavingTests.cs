namespace FrozenArrow.Tests.Linq;

/// <summary>
/// Tests to verify LINQ HAVING equivalent support (post-aggregation filtering).
/// In LINQ, HAVING is achieved with .Where() after .GroupBy().
/// </summary>
public class LinqHavingTests
{
    [ArrowRecord]
    public record HavingTestData
    {
        [ArrowArray(Name = "Id")]
        public int Id { get; init; }

        [ArrowArray(Name = "Category")]
        public string Category { get; init; } = string.Empty;

        [ArrowArray(Name = "Amount")]
        public double Amount { get; init; }
    }

    private static FrozenArrow<HavingTestData> CreateTestData()
    {
        return new List<HavingTestData>
        {
            // Category A: 5 items, total 250
            new() { Id = 1, Category = "A", Amount = 50 },
            new() { Id = 2, Category = "A", Amount = 60 },
            new() { Id = 3, Category = "A", Amount = 40 },
            new() { Id = 4, Category = "A", Amount = 70 },
            new() { Id = 5, Category = "A", Amount = 30 },
            
            // Category B: 2 items, total 120
            new() { Id = 6, Category = "B", Amount = 80 },
            new() { Id = 7, Category = "B", Amount = 40 },
            
            // Category C: 8 items, total 400
            new() { Id = 8, Category = "C", Amount = 50 },
            new() { Id = 9, Category = "C", Amount = 50 },
            new() { Id = 10, Category = "C", Amount = 50 },
            new() { Id = 11, Category = "C", Amount = 50 },
            new() { Id = 12, Category = "C", Amount = 50 },
            new() { Id = 13, Category = "C", Amount = 50 },
            new() { Id = 14, Category = "C", Amount = 50 },
            new() { Id = 15, Category = "C", Amount = 50 }
        }.ToFrozenArrow();
    }

    [Fact]
    public void LinqQuery_GroupByWithCountFilter_WorksCorrectly()
    {
        // Arrange - HAVING COUNT(*) > 3
        var data = CreateTestData();

        // Act - LINQ equivalent of HAVING
        var result = data.AsQueryable()
            .GroupBy(x => x.Category)
            .Where(g => g.Count() > 3)  // This is HAVING in LINQ
            .Select(g => new { Category = g.Key, Count = g.Count() })
            .ToList();

        // Assert
        Assert.Equal(2, result.Count); // A (5 items), C (8 items)
        Assert.Contains(result, r => r.Category == "A" && r.Count == 5);
        Assert.Contains(result, r => r.Category == "C" && r.Count == 8);
    }

    [Fact]
    public void LinqQuery_GroupByWithSumFilter_WorksCorrectly()
    {
        // Arrange - HAVING SUM(Amount) > 200
        var data = CreateTestData();

        // Act
        var result = data.AsQueryable()
            .GroupBy(x => x.Category)
            .Select(g => new { Category = g.Key, Total = g.Sum(x => x.Amount) })
            .Where(x => x.Total > 200)  // Filter after aggregation
            .ToList();

        // Assert
        Assert.Equal(2, result.Count); // A (250), C (400)
        Assert.Contains(result, r => r.Category == "A" && r.Total == 250);
        Assert.Contains(result, r => r.Category == "C" && r.Total == 400);
    }

    [Fact]
    public void LinqQuery_GroupByWithAvgFilter_WorksCorrectly()
    {
        // Arrange - HAVING AVG(Amount) >= 50
        var data = CreateTestData();

        // Act
        var result = data.AsQueryable()
            .GroupBy(x => x.Category)
            .Select(g => new { Category = g.Key, Avg = g.Average(x => x.Amount), Count = g.Count() })
            .Where(x => x.Avg >= 50)
            .ToList();

        // Assert
        Assert.Equal(3, result.Count); // A (avg=50), B (avg=60), C (avg=50)
        Assert.Contains(result, r => r.Category == "A");
        Assert.Contains(result, r => r.Category == "B");
        Assert.Contains(result, r => r.Category == "C");
    }

    [Fact]
    public void LinqQuery_ComplexHavingEquivalent_WorksCorrectly()
    {
        // Arrange - HAVING COUNT(*) >= 3 AND SUM(Amount) > 200
        var data = CreateTestData();

        // Act
        var result = data.AsQueryable()
            .GroupBy(x => x.Category)
            .Select(g => new 
            { 
                Category = g.Key, 
                Count = g.Count(), 
                Total = g.Sum(x => x.Amount) 
            })
            .Where(x => x.Count >= 3 && x.Total > 200)
            .ToList();

        // Assert
        Assert.Equal(2, result.Count); // A (5 items, 250), C (8 items, 400)
        Assert.All(result, r => Assert.True(r.Count >= 3 && r.Total > 200));
    }
}
