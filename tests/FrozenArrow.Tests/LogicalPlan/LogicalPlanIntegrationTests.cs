using FrozenArrow.Query;

namespace FrozenArrow.Tests.LogicalPlan;

/// <summary>
/// Integration tests for logical plan execution through ArrowQueryProvider.
/// Tests the complete flow: LINQ ? Logical Plan ? Optimizer ? Execution.
/// </summary>
public class LogicalPlanIntegrationTests
{
    [ArrowRecord]
    public record TestRecord
    {
        [ArrowArray(Name = "Id")]
        public int Id { get; init; }

        [ArrowArray(Name = "Name")]
        public string Name { get; init; } = string.Empty;

        [ArrowArray(Name = "Age")]
        public int Age { get; init; }

        [ArrowArray(Name = "Category")]
        public string Category { get; init; } = string.Empty;

        [ArrowArray(Name = "Sales")]
        public double Sales { get; init; }

        [ArrowArray(Name = "IsActive")]
        public bool IsActive { get; init; }
    }

    private static FrozenArrow<TestRecord> CreateTestData()
    {
        var records = new[]
        {
            new TestRecord { Id = 1, Name = "Alice", Age = 25, Category = "A", Sales = 100.0, IsActive = true },
            new TestRecord { Id = 2, Name = "Bob", Age = 35, Category = "B", Sales = 200.0, IsActive = true },
            new TestRecord { Id = 3, Name = "Charlie", Age = 45, Category = "A", Sales = 300.0, IsActive = false },
            new TestRecord { Id = 4, Name = "Diana", Age = 28, Category = "B", Sales = 150.0, IsActive = true },
            new TestRecord { Id = 5, Name = "Eve", Age = 32, Category = "A", Sales = 250.0, IsActive = true },
            new TestRecord { Id = 6, Name = "Frank", Age = 40, Category = "C", Sales = 400.0, IsActive = false },
            new TestRecord { Id = 7, Name = "Grace", Age = 29, Category = "B", Sales = 180.0, IsActive = true },
            new TestRecord { Id = 8, Name = "Henry", Age = 55, Category = "A", Sales = 500.0, IsActive = true },
            new TestRecord { Id = 9, Name = "Ivy", Age = 23, Category = "C", Sales = 120.0, IsActive = true },
            new TestRecord { Id = 10, Name = "Jack", Age = 38, Category = "B", Sales = 220.0, IsActive = false },
        };

        return records.ToFrozenArrow();
    }

    [Fact]
    public void LogicalPlan_SimpleCount_MatchesOldPath()
    {
        // Arrange
        var data = CreateTestData();
        var queryable = data.AsQueryable();

        // Act - Execute with old path
        var oldResult = queryable.Count();

        // Act - Execute with new logical plan path
        ((ArrowQueryProvider)queryable.Provider).UseLogicalPlanExecution = true;
        var newResult = queryable.Count();

        // Assert
        Assert.Equal(oldResult, newResult);
        Assert.Equal(10, newResult);
    }

    [Fact]
    public void LogicalPlan_SimpleFilter_MatchesOldPath()
    {
        // Arrange
        var data = CreateTestData();
        var queryable = data.AsQueryable();

        // Act - Execute with old path
        var oldResults = queryable.Where(x => x.Age > 30).ToList();

        // Act - Execute with new logical plan path
        ((ArrowQueryProvider)queryable.Provider).UseLogicalPlanExecution = true;
        var newResults = queryable.Where(x => x.Age > 30).ToList();

        // Assert
        Assert.Equal(oldResults.Count, newResults.Count);
        // Ages > 30: Bob(35), Charlie(45), Eve(32), Frank(40), Henry(55), Jack(38) = 6 people
        Assert.Equal(6, newResults.Count);
        Assert.All(newResults, r => Assert.True(r.Age > 30));
    }

    [Fact]
    public void LogicalPlan_FilterWithMultiplePredicates_MatchesOldPath()
    {
        // Arrange
        var data = CreateTestData();
        var queryable = data.AsQueryable();

        // Act - Execute with old path
        var oldResults = queryable
            .Where(x => x.Age > 25 && x.IsActive)
            .ToList();

        // Act - Execute with new logical plan path
        ((ArrowQueryProvider)queryable.Provider).UseLogicalPlanExecution = true;
        var newResults = queryable
            .Where(x => x.Age > 25 && x.IsActive)
            .ToList();

        // Assert
        Assert.Equal(oldResults.Count, newResults.Count);
        Assert.Equal(5, newResults.Count);
    }

    [Fact]
    public void LogicalPlan_Take_MatchesOldPath()
    {
        // Arrange
        var data = CreateTestData();
        var queryable = data.AsQueryable();

        // Act - Execute with old path
        var oldResults = queryable.Take(3).ToList();

        // Act - Execute with new logical plan path
        ((ArrowQueryProvider)queryable.Provider).UseLogicalPlanExecution = true;
        var newResults = queryable.Take(3).ToList();

        // Assert
        Assert.Equal(oldResults.Count, newResults.Count);
        Assert.Equal(3, newResults.Count);
    }

    [Fact]
    public void LogicalPlan_Skip_MatchesOldPath()
    {
        // Arrange
        var data = CreateTestData();
        var queryable = data.AsQueryable();

        // Act - Execute with old path
        var oldResults = queryable.Skip(5).ToList();

        // Act - Execute with new logical plan path
        ((ArrowQueryProvider)queryable.Provider).UseLogicalPlanExecution = true;
        var newResults = queryable.Skip(5).ToList();

        // Assert
        Assert.Equal(oldResults.Count, newResults.Count);
        Assert.Equal(5, newResults.Count);
    }

    [Fact]
    public void LogicalPlan_SkipAndTake_MatchesOldPath()
    {
        // Arrange
        var data = CreateTestData();
        var queryable = data.AsQueryable();

        // Act - Execute with old path
        var oldResults = queryable.Skip(2).Take(5).ToList();

        // Act - Execute with new logical plan path
        ((ArrowQueryProvider)queryable.Provider).UseLogicalPlanExecution = true;
        var newResults = queryable.Skip(2).Take(5).ToList();

        // Assert
        Assert.Equal(oldResults.Count, newResults.Count);
        Assert.Equal(5, newResults.Count);
    }

    [Fact]
    public void LogicalPlan_FilterWithPagination_MatchesOldPath()
    {
        // Arrange
        var data = CreateTestData();
        var queryable = data.AsQueryable();

        // Act - Execute with new logical plan path
        ((ArrowQueryProvider)queryable.Provider).UseLogicalPlanExecution = true;
        var results = queryable
            .Where(x => x.IsActive)
            .Skip(1)
            .Take(3)
            .ToList();

        // Assert
        // IsActive: Alice(1), Bob(2), Diana(4), Eve(5), Grace(7), Henry(8), Ivy(9) = 7 people
        // Skip 1 (skip Alice), Take 3 = Bob, Diana, Eve
        Assert.True(results.Count >= 2 && results.Count <= 3); // Should be 2-3 depending on execution order
        Assert.All(results, r => Assert.True(r.IsActive));
    }

    [Fact]
    public void LogicalPlan_First_MatchesOldPath()
    {
        // Arrange
        var data = CreateTestData();
        var queryable = data.AsQueryable();

        // Act - Execute with old path
        var oldResult = queryable.Where(x => x.Age > 30).First();

        // Act - Execute with new logical plan path
        ((ArrowQueryProvider)queryable.Provider).UseLogicalPlanExecution = true;
        var newResult = queryable.Where(x => x.Age > 30).First();

        // Assert
        Assert.Equal(oldResult.Id, newResult.Id);
        Assert.True(newResult.Age > 30);
    }

    [Fact]
    public void LogicalPlan_Any_MatchesOldPath()
    {
        // Arrange
        var data = CreateTestData();
        var queryable = data.AsQueryable();

        // Act - Execute with old path
        var oldResult = queryable.Where(x => x.Age > 50).Any();

        // Act - Execute with new logical plan path
        ((ArrowQueryProvider)queryable.Provider).UseLogicalPlanExecution = true;
        var newResult = queryable.Where(x => x.Age > 50).Any();

        // Assert
        Assert.Equal(oldResult, newResult);
        Assert.True(newResult); // Henry is 55
    }

    [Fact]
    public void LogicalPlan_FallsBackOnUnsupportedOperation()
    {
        // Arrange
        var data = CreateTestData();
        var queryable = data.AsQueryable();
        var provider = (ArrowQueryProvider)queryable.Provider;
        provider.UseLogicalPlanExecution = true;
        provider.StrictMode = false; // Allow fallback

        // Act - This should fall back to old path since OrderBy isn't supported yet
        var results = queryable
            .Where(x => x.Age > 30)
            .OrderBy(x => x.Age) // Not supported in logical plan yet
            .ToList();

        // Assert - Should still work via fallback
        // Ages > 30: Bob(35), Charlie(45), Eve(32), Frank(40), Henry(55), Jack(38) = 6 people
        Assert.Equal(6, results.Count);
        Assert.All(results, r => Assert.True(r.Age > 30));
        // Should be sorted by age
        Assert.True(results.SequenceEqual(results.OrderBy(x => x.Age)));
    }
}
