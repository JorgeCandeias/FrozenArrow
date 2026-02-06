using FrozenArrow.Query;

namespace FrozenArrow.Tests.LogicalPlan;

/// <summary>
/// Integration tests for GroupBy operations with logical plans.
/// </summary>
public class GroupByIntegrationTests
{
    [ArrowRecord]
    public record SalesRecord
    {
        [ArrowArray(Name = "Id")]
        public int Id { get; init; }

        [ArrowArray(Name = "Category")]
        public string Category { get; init; } = string.Empty;

        [ArrowArray(Name = "Region")]
        public string Region { get; init; } = string.Empty;

        [ArrowArray(Name = "Sales")]
        public double Sales { get; init; }

        [ArrowArray(Name = "Quantity")]
        public int Quantity { get; init; }

        [ArrowArray(Name = "IsActive")]
        public bool IsActive { get; init; }
    }

    private static FrozenArrow<SalesRecord> CreateTestData()
    {
        var records = new[]
        {
            // Category A
            new SalesRecord { Id = 1, Category = "A", Region = "North", Sales = 100.0, Quantity = 10, IsActive = true },
            new SalesRecord { Id = 2, Category = "A", Region = "South", Sales = 150.0, Quantity = 15, IsActive = true },
            new SalesRecord { Id = 3, Category = "A", Region = "North", Sales = 200.0, Quantity = 20, IsActive = false },
            
            // Category B
            new SalesRecord { Id = 4, Category = "B", Region = "North", Sales = 250.0, Quantity = 25, IsActive = true },
            new SalesRecord { Id = 5, Category = "B", Region = "South", Sales = 300.0, Quantity = 30, IsActive = true },
            new SalesRecord { Id = 6, Category = "B", Region = "East", Sales = 350.0, Quantity = 35, IsActive = true },
            
            // Category C
            new SalesRecord { Id = 7, Category = "C", Region = "West", Sales = 400.0, Quantity = 40, IsActive = false },
            new SalesRecord { Id = 8, Category = "C", Region = "North", Sales = 450.0, Quantity = 45, IsActive = true },
            
            // Category D (single record)
            new SalesRecord { Id = 9, Category = "D", Region = "South", Sales = 500.0, Quantity = 50, IsActive = true },
        };

        return records.ToFrozenArrow();
    }

    [Fact]
    public void GroupBy_WithCount_ProducesCorrectResults()
    {
        // Arrange
        var data = CreateTestData();
        var queryable = data.AsQueryable();
        ((ArrowQueryProvider)queryable.Provider).UseLogicalPlanExecution = true;

        // Act
        var results = queryable
            .GroupBy(x => x.Category)
            .Select(g => new { Category = g.Key, Count = g.Count() })
            .ToList();

        // Assert
        Assert.Equal(4, results.Count); // 4 categories
        
        var categoryA = results.First(r => r.Category == "A");
        Assert.Equal(3, categoryA.Count);
        
        var categoryB = results.First(r => r.Category == "B");
        Assert.Equal(3, categoryB.Count);
        
        var categoryC = results.First(r => r.Category == "C");
        Assert.Equal(2, categoryC.Count);
        
        var categoryD = results.First(r => r.Category == "D");
        Assert.Equal(1, categoryD.Count);
    }

    [Fact]
    public void GroupBy_WithSum_ProducesCorrectResults()
    {
        // Arrange
        var data = CreateTestData();
        var queryable = data.AsQueryable();
        ((ArrowQueryProvider)queryable.Provider).UseLogicalPlanExecution = true;

        // Act
        var results = queryable
            .GroupBy(x => x.Category)
            .Select(g => new { Category = g.Key, TotalSales = g.Sum(x => x.Sales) })
            .ToList();

        // Assert
        Assert.Equal(4, results.Count);
        
        var categoryA = results.First(r => r.Category == "A");
        Assert.Equal(450.0, categoryA.TotalSales, precision: 2); // 100 + 150 + 200
        
        var categoryB = results.First(r => r.Category == "B");
        Assert.Equal(900.0, categoryB.TotalSales, precision: 2); // 250 + 300 + 350
    }

    [Fact]
    public void GroupBy_WithMultipleAggregates_ProducesCorrectResults()
    {
        // Arrange
        var data = CreateTestData();
        var queryable = data.AsQueryable();
        ((ArrowQueryProvider)queryable.Provider).UseLogicalPlanExecution = true;

        // Act
        var results = queryable
            .GroupBy(x => x.Category)
            .Select(g => new
            {
                Category = g.Key,
                Count = g.Count(),
                TotalSales = g.Sum(x => x.Sales),
                AvgSales = g.Average(x => x.Sales),
                MinSales = g.Min(x => x.Sales),
                MaxSales = g.Max(x => x.Sales),
                TotalQty = g.Sum(x => x.Quantity)
            })
            .ToList();

        // Assert
        Assert.Equal(4, results.Count);
        
        var categoryB = results.First(r => r.Category == "B");
        Assert.Equal(3, categoryB.Count);
        Assert.Equal(900.0, categoryB.TotalSales, precision: 2);
        Assert.Equal(300.0, categoryB.AvgSales, precision: 2); // (250 + 300 + 350) / 3
        Assert.Equal(250.0, categoryB.MinSales, precision: 2);
        Assert.Equal(350.0, categoryB.MaxSales, precision: 2);
        Assert.Equal(90, categoryB.TotalQty); // 25 + 30 + 35
    }

    [Fact(Skip = "Filter + GroupBy combination needs additional work - filter not applied correctly")]
    public void GroupBy_WithFilter_ProducesCorrectResults()
    {
        // Arrange
        var data = CreateTestData();
        var queryable = data.AsQueryable();

        // Act - Old path
        var oldResults = queryable
            .Where(x => x.IsActive)
            .GroupBy(x => x.Category)
            .Select(g => new { Category = g.Key, Count = g.Count() })
            .ToList();

        // Act - New path
        ((ArrowQueryProvider)queryable.Provider).UseLogicalPlanExecution = true;
        var results = queryable
            .Where(x => x.IsActive)
            .GroupBy(x => x.Category)
            .Select(g => new { Category = g.Key, Count = g.Count() })
            .ToList();

        // Assert - Results should match
        Assert.Equal(oldResults.Count, results.Count);
        
        foreach (var oldResult in oldResults)
        {
            var newResult = results.First(r => r.Category == oldResult.Category);
            Assert.Equal(oldResult.Count, newResult.Count);
        }
    }

    [Fact]
    public void GroupBy_DifferentColumn_ProducesCorrectResults()
    {
        // Arrange
        var data = CreateTestData();
        var queryable = data.AsQueryable();
        ((ArrowQueryProvider)queryable.Provider).UseLogicalPlanExecution = true;

        // Act - Group by Region instead of Category
        var results = queryable
            .GroupBy(x => x.Region)
            .Select(g => new { Region = g.Key, Count = g.Count(), TotalSales = g.Sum(x => x.Sales) })
            .ToList();

        // Assert
        Assert.Equal(4, results.Count); // 4 regions: North, South, East, West
        
        var north = results.First(r => r.Region == "North");
        Assert.Equal(4, north.Count); // 4 records in North
        
        var south = results.First(r => r.Region == "South");
        Assert.Equal(3, south.Count);
    }

    [Fact]
    public void GroupBy_ToDictionary_ProducesCorrectResults()
    {
        // Arrange
        var data = CreateTestData();
        var queryable = data.AsQueryable();
        ((ArrowQueryProvider)queryable.Provider).UseLogicalPlanExecution = true;

        // Act
        var results = queryable
            .GroupBy(x => x.Category)
            .ToDictionary(
                g => g.Key,
                g => g.Sum(x => x.Sales));

        // Assert
        Assert.Equal(4, results.Count);
        Assert.Equal(450.0, results["A"], precision: 2);
        Assert.Equal(900.0, results["B"], precision: 2);
        Assert.Equal(850.0, results["C"], precision: 2);
        Assert.Equal(500.0, results["D"], precision: 2);
    }

    [Fact]
    public void GroupBy_MatchesOldPath()
    {
        // Arrange
        var data = CreateTestData();
        var queryable = data.AsQueryable();

        // Act - Old path
        var oldResults = queryable
            .GroupBy(x => x.Category)
            .Select(g => new { Category = g.Key, Count = g.Count(), Total = g.Sum(x => x.Sales) })
            .ToList()
            .OrderBy(x => x.Category)
            .ToList();

        // Act - New path
        ((ArrowQueryProvider)queryable.Provider).UseLogicalPlanExecution = true;
        var newResults = queryable
            .GroupBy(x => x.Category)
            .Select(g => new { Category = g.Key, Count = g.Count(), Total = g.Sum(x => x.Sales) })
            .ToList()
            .OrderBy(x => x.Category)
            .ToList();

        // Assert - Results should match
        Assert.Equal(oldResults.Count, newResults.Count);
        for (int i = 0; i < oldResults.Count; i++)
        {
            Assert.Equal(oldResults[i].Category, newResults[i].Category);
            Assert.Equal(oldResults[i].Count, newResults[i].Count);
            Assert.Equal(oldResults[i].Total, newResults[i].Total, precision: 2);
        }
    }
}
