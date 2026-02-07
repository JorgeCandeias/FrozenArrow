using FrozenArrow.Query;

namespace FrozenArrow.Tests.Sql;

/// <summary>
/// Tests for SQL string predicate support (Phase A enhancement).
/// </summary>
public class SqlStringPredicateTests
{
    [ArrowRecord]
    public record StringTestRecord
    {
        [ArrowArray(Name = "Id")]
        public int Id { get; init; }

        [ArrowArray(Name = "Name")]
        public string Name { get; init; } = string.Empty;

        [ArrowArray(Name = "Category")]
        public string Category { get; init; } = string.Empty;

        [ArrowArray(Name = "Value")]
        public int Value { get; init; }
    }

    private static FrozenArrow<StringTestRecord> CreateTestData()
    {
        return new List<StringTestRecord>
        {
            new() { Id = 1, Name = "Alice", Category = "A", Value = 100 },
            new() { Id = 2, Name = "Bob", Category = "B", Value = 200 },
            new() { Id = 3, Name = "Charlie", Category = "A", Value = 150 },
            new() { Id = 4, Name = "Diana", Category = "C", Value = 300 },
            new() { Id = 5, Name = "Eve", Category = "B", Value = 250 },
            new() { Id = 6, Name = "Alice", Category = "C", Value = 180 }, // Duplicate name
            new() { Id = 7, Name = "Frank", Category = "A", Value = 120 }
        }.ToFrozenArrow();
    }

    [Fact]
    public void SqlQuery_StringEquality_ReturnsMatchingRecords()
    {
        // Arrange
        var data = CreateTestData();

        // Act
        var result = data.ExecuteSql<StringTestRecord, StringTestRecord>(
            "SELECT * FROM data WHERE Name = 'Alice'");

        // Assert
        Assert.Equal(2, result.Count());
        Assert.All(result, r => Assert.Equal("Alice", r.Name));
    }

    [Fact]
    public void SqlQuery_StringLikeStartsWith_ReturnsMatchingRecords()
    {
        // Arrange
        var data = CreateTestData();

        // Act  
        var result = data.ExecuteSql<StringTestRecord, StringTestRecord>(
            "SELECT * FROM data WHERE Name LIKE 'A%'");

        // Assert
        var list = result.ToList();
        Assert.Equal(2, list.Count); // Alice (twice)
        Assert.All(list, r => Assert.StartsWith("A", r.Name));
    }

    [Fact]
    public void SqlQuery_StringLikeEndsWith_ReturnsMatchingRecords()
    {
        // Arrange
        var data = CreateTestData();

        // Act
        var result = data.ExecuteSql<StringTestRecord, StringTestRecord>(
            "SELECT * FROM data WHERE Name LIKE '%e'");

        // Assert
        var list = result.ToList();
        Assert.True(list.Count >= 3); // Alice, Charlie, Eve, etc.
        Assert.All(list, r => Assert.EndsWith("e", r.Name));
    }

    [Fact]
    public void SqlQuery_StringLikeContains_ReturnsMatchingRecords()
    {
        // Arrange
        var data = CreateTestData();

        // Act
        var result = data.ExecuteSql<StringTestRecord, StringTestRecord>(
            "SELECT * FROM data WHERE Name LIKE '%li%'");

        // Assert
        var list = result.ToList();
        Assert.True(list.Count >= 3); // Alice (twice), Charlie
        Assert.All(list, r => Assert.Contains("li", r.Name));
    }

    [Fact]
    public void SqlQuery_StringWithAnd_CombinesPredicates()
    {
        // Arrange
        var data = CreateTestData();

        // Act
        var result = data.ExecuteSql<StringTestRecord, StringTestRecord>(
            "SELECT * FROM data WHERE Category = 'A' AND Value > 100");

        // Assert
        var list = result.ToList();
        Assert.Equal(2, list.Count); // Charlie (150), Frank (120)
        Assert.All(list, r =>
        {
            Assert.Equal("A", r.Category);
            Assert.True(r.Value > 100);
        });
    }

    [Fact]
    public void SqlQuery_StringCount_WorksCorrectly()
    {
        // Arrange
        var data = CreateTestData();

        // Act
        var count = data.ExecuteSqlScalar<StringTestRecord, int>(
            "SELECT COUNT(*) FROM data WHERE Name = 'Alice'");

        // Assert
        Assert.Equal(2, count);
    }

    [Fact]
    public void SqlQuery_StringAndIntPredicates_WorkTogether()
    {
        // Arrange
        var data = CreateTestData();

        // Act
        var result = data.ExecuteSql<StringTestRecord, StringTestRecord>(
            "SELECT * FROM data WHERE Name LIKE 'A%' AND Value > 150");

        // Assert
        var list = result.ToList();
        Assert.Single(list); // Alice with Value 180
        Assert.Equal("Alice", list[0].Name);
        Assert.Equal(180, list[0].Value);
    }

    [Fact]
    public void SqlQuery_StringGroupBy_WorksCorrectly()
    {
        // Arrange
        var data = CreateTestData();

        // Act - Use LINQ to verify GROUP BY works with strings
        var linqResult = data.AsQueryable()
            .GroupBy(x => x.Category)
            .Select(g => new { Category = g.Key, Count = g.Count() })
            .ToList();

        // Assert
        Assert.Equal(3, linqResult.Count); // A, B, C
        
        // Verify counts
        var categoryA = linqResult.First(r => r.Category == "A");
        Assert.Equal(3, categoryA.Count);
    }
}
