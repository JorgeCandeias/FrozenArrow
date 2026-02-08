namespace FrozenArrow.Tests.Sql;

/// <summary>
/// Tests for SQL HAVING clause support (Phase B Quick Win - HAVING).
/// </summary>
public class SqlHavingTests
{
    [ArrowRecord]
    public record SqlHavingTestData
    {
        [ArrowArray(Name = "Id")]
        public int Id { get; init; }

        [ArrowArray(Name = "Category")]
        public string Category { get; init; } = string.Empty;

        [ArrowArray(Name = "Amount")]
        public double Amount { get; init; }
    }

    private static FrozenArrow<SqlHavingTestData> CreateTestData()
    {
        return new List<SqlHavingTestData>
        {
            // Category A: 5 items, total 250, avg 50
            new() { Id = 1, Category = "A", Amount = 50 },
            new() { Id = 2, Category = "A", Amount = 60 },
            new() { Id = 3, Category = "A", Amount = 40 },
            new() { Id = 4, Category = "A", Amount = 70 },
            new() { Id = 5, Category = "A", Amount = 30 },
            
            // Category B: 2 items, total 120, avg 60
            new() { Id = 6, Category = "B", Amount = 80 },
            new() { Id = 7, Category = "B", Amount = 40 },
            
            // Category C: 8 items, total 400, avg 50
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
    public void SqlParser_HavingClause_ParsesCorrectly()
    {
        // Arrange
        var schema = new Dictionary<string, Type>
        {
            ["Category"] = typeof(string),
            ["Amount"] = typeof(double)
        };
        
        var columnIndexMap = new Dictionary<string, int>
        {
            ["Category"] = 0,
            ["Amount"] = 1
        };

        var parser = new FrozenArrow.Query.Sql.SqlParser(schema, columnIndexMap, 100);

        // Act
        var sql = "SELECT Category, COUNT(*) as Count FROM data GROUP BY Category HAVING Category = 'A'";
        var plan = parser.Parse(sql);

        // Assert - Verify it parses without error
        Assert.NotNull(plan);
        Console.WriteLine($"Plan: {plan}");
    }

    // Note: Full SQL HAVING with aggregate functions (COUNT(*), SUM, etc.) in the HAVING clause
    // requires more complex parsing than what's currently implemented.
    // The simplified implementation supports filtering on the GROUP BY key column.
    
    // For full aggregate function support in HAVING, use LINQ:
    // .GroupBy(x => x.Category)
    // .Where(g => g.Count() > 3)
}
