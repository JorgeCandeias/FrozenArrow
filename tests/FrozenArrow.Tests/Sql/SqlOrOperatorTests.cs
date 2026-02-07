using FrozenArrow.Query;

namespace FrozenArrow.Tests.Sql;

/// <summary>
/// Tests for SQL OR operator support (Phase A Part 2).
/// </summary>
public class SqlOrOperatorTests
{
    [ArrowRecord]
    public record OrTestData
    {
        [ArrowArray(Name = "Id")]
        public int Id { get; init; }

        [ArrowArray(Name = "Value")]
        public int Value { get; init; }
    }

    [Fact]
    public void SqlQuery_SimpleOr_Works()
    {
        // Arrange
        var data = new List<OrTestData>
        {
            new() { Id = 1, Value = 100 },
            new() { Id = 2, Value = 200 },
            new() { Id = 3, Value = 300 }
        }.ToFrozenArrow();

        // Act
        var result = data.ExecuteSql<OrTestData, OrTestData>(
            "SELECT * FROM data WHERE Value = 100 OR Value = 300");

        // Assert
        var list = result.ToList();
        
        // Debug: Check what we got
        Console.WriteLine($"Result count: {list.Count}");
        Console.WriteLine($"Values: {string.Join(", ", list.Select(x => x.Value))}");
        
        Assert.Equal(2, list.Count);
        Assert.Contains(list, r => r.Value == 100);
        Assert.Contains(list, r => r.Value == 300);
    }

    [Fact]
    public void SqlQuery_MultipleOr_Works()
    {
        // Arrange
        var data = new List<OrTestData>
        {
            new() { Id = 1, Value = 10 },
            new() { Id = 2, Value = 20 },
            new() { Id = 3, Value = 30 },
            new() { Id = 4, Value = 40 }
        }.ToFrozenArrow();

        // Act
        var result = data.ExecuteSql<OrTestData, OrTestData>(
            "SELECT * FROM data WHERE Value = 10 OR Value = 20 OR Value = 40");

        // Assert
        var list = result.ToList();
        Assert.Equal(3, list.Count);
    }
}
