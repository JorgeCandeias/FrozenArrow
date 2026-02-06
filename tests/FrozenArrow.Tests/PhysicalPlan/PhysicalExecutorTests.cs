using FrozenArrow.Query;
using FrozenArrow.Query.PhysicalPlan;

namespace FrozenArrow.Tests.PhysicalPlan;

/// <summary>
/// Tests for physical plan execution with strategy-specific behavior.
/// </summary>
public class PhysicalExecutorTests
{
    [ArrowRecord]
    public record PhysicalTestRecord
    {
        [ArrowArray(Name = "Id")]
        public int Id { get; init; }

        [ArrowArray(Name = "Value")]
        public int Value { get; init; }

        [ArrowArray(Name = "Category")]
        public string Category { get; init; } = string.Empty;

        [ArrowArray(Name = "Score")]
        public double Score { get; init; }
    }

    private static FrozenArrow<PhysicalTestRecord> CreateTestData()
    {
        var records = new[]
        {
            new PhysicalTestRecord { Id = 1, Value = 100, Category = "A", Score = 85.5 },
            new PhysicalTestRecord { Id = 2, Value = 200, Category = "B", Score = 92.0 },
            new PhysicalTestRecord { Id = 3, Value = 300, Category = "A", Score = 78.3 },
            new PhysicalTestRecord { Id = 4, Value = 400, Category = "B", Score = 88.7 },
            new PhysicalTestRecord { Id = 5, Value = 500, Category = "C", Score = 95.2 },
        };

        return records.ToFrozenArrow();
    }

    [Fact]
    public void PhysicalExecutor_SimpleFilter_ReturnsCorrectResults()
    {
        // Arrange
        var data = CreateTestData();
        var queryable = data.AsQueryable();
        var provider = (ArrowQueryProvider)queryable.Provider;

        // Enable physical plan execution
        provider.UseLogicalPlanExecution = true;
        provider.UsePhysicalPlanExecution = true;

        // Act
        var results = queryable.Where(x => x.Value > 200).ToList();

        // Assert
        Assert.Equal(3, results.Count);
        Assert.All(results, r => Assert.True(r.Value > 200));
    }

    [Fact]
    public void PhysicalExecutor_MatchesDirectExecution()
    {
        // Arrange
        var data = CreateTestData();
        var queryable = data.AsQueryable();
        var provider = (ArrowQueryProvider)queryable.Provider;
        provider.UseLogicalPlanExecution = true;

        // Act - Direct execution
        provider.UsePhysicalPlanExecution = false;
        provider.UseDirectLogicalPlanExecution = true;
        var directResults = queryable.Where(x => x.Value > 200 && x.Value < 500).ToList();

        // Act - Physical execution
        provider.UsePhysicalPlanExecution = true;
        provider.UseDirectLogicalPlanExecution = false;
        var physicalResults = queryable.Where(x => x.Value > 200 && x.Value < 500).ToList();

        // Assert - Results should match
        Assert.Equal(directResults.Count, physicalResults.Count);
        Assert.Equal(2, physicalResults.Count);
    }

    [Fact]
    public void PhysicalExecutor_Count_WorksCorrectly()
    {
        // Arrange
        var data = CreateTestData();
        var queryable = data.AsQueryable();
        var provider = (ArrowQueryProvider)queryable.Provider;
        provider.UseLogicalPlanExecution = true;
        provider.UsePhysicalPlanExecution = true;

        // Act
        var count = queryable.Where(x => x.Category == "A").Count();

        // Assert
        Assert.Equal(2, count);
    }

    [Fact]
    public void PhysicalExecutor_GroupBy_WorksCorrectly()
    {
        // Arrange
        var data = CreateTestData();
        var queryable = data.AsQueryable();
        var provider = (ArrowQueryProvider)queryable.Provider;
        provider.UseLogicalPlanExecution = true;
        provider.UsePhysicalPlanExecution = true;

        // Act
        var results = queryable
            .GroupBy(x => x.Category)
            .Select(g => new { Category = g.Key, Count = g.Count() })
            .ToList()
            .OrderBy(x => x.Category)
            .ToList();

        // Assert
        Assert.Equal(3, results.Count);
        Assert.Equal("A", results[0].Category);
        Assert.Equal(2, results[0].Count);
        Assert.Equal("B", results[1].Category);
        Assert.Equal(2, results[1].Count);
        Assert.Equal("C", results[2].Category);
        Assert.Equal(1, results[2].Count);
    }

    [Fact]
    public void PhysicalExecutor_ComplexQuery_WorksCorrectly()
    {
        // Arrange
        var data = CreateTestData();
        var queryable = data.AsQueryable();
        var provider = (ArrowQueryProvider)queryable.Provider;
        provider.UseLogicalPlanExecution = true;
        provider.UsePhysicalPlanExecution = true;

        // Act - Filter + GroupBy + Aggregations
        var results = queryable
            .Where(x => x.Score > 80)
            .GroupBy(x => x.Category)
            .Select(g => new
            {
                Category = g.Key,
                Count = g.Count(),
                AvgScore = g.Average(x => x.Score)
            })
            .ToList()
            .OrderBy(x => x.Category)
            .ToList();

        // Assert
        Assert.Equal(3, results.Count); // A, B, C all have records with Score > 80
        
        var categoryA = results.First(r => r.Category == "A");
        Assert.Equal(1, categoryA.Count); // Only Id=1 has Score > 80
        Assert.Equal(85.5, categoryA.AvgScore, precision: 1);

        var categoryB = results.First(r => r.Category == "B");
        Assert.Equal(2, categoryB.Count);
    }

    [Fact]
    public void PhysicalExecutor_FallsBackOnError()
    {
        // Arrange
        var data = CreateTestData();
        var queryable = data.AsQueryable();
        var provider = (ArrowQueryProvider)queryable.Provider;
        provider.UseLogicalPlanExecution = true;
        provider.UsePhysicalPlanExecution = true;

        // Act - Should fall back gracefully even if physical execution has issues
        var count = queryable.Count();

        // Assert - Should still work (via fallback)
        Assert.Equal(5, count);
    }
}
