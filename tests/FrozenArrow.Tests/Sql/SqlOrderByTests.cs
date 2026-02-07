namespace FrozenArrow.Tests.Sql;

/// <summary>
/// Tests for SQL ORDER BY support (Phase B).
/// </summary>
public class SqlOrderByTests
{
    [ArrowRecord]
    public record OrderByTestData
    {
        [ArrowArray(Name = "Id")]
        public int Id { get; init; }

        [ArrowArray(Name = "Name")]
        public string Name { get; init; } = string.Empty;

        [ArrowArray(Name = "Score")]
        public double Score { get; init; }

        [ArrowArray(Name = "Category")]
        public string Category { get; init; } = string.Empty;
    }

    private static FrozenArrow<OrderByTestData> CreateTestData()
    {
        return new List<OrderByTestData>
        {
            new() { Id = 1, Name = "Charlie", Score = 85.5, Category = "B" },
            new() { Id = 2, Name = "Alice", Score = 92.0, Category = "A" },
            new() { Id = 3, Name = "Bob", Score = 78.5, Category = "A" },
            new() { Id = 4, Name = "Diana", Score = 95.0, Category = "C" },
            new() { Id = 5, Name = "Eve", Score = 88.0, Category = "B" }
        }.ToFrozenArrow();
    }

    [Fact]
    public void SqlQuery_OrderByAscending_SortsCorrectly()
    {
        // Arrange
        var data = CreateTestData();

        // Act
        var result = data.ExecuteSql<OrderByTestData, OrderByTestData>(
            "SELECT * FROM data ORDER BY Name");

        // Assert
        var list = result.ToList();
        Assert.Equal(5, list.Count);
        
        // Should be in alphabetical order by Name
        Assert.Equal("Alice", list[0].Name);
        Assert.Equal("Bob", list[1].Name);
        Assert.Equal("Charlie", list[2].Name);
        Assert.Equal("Diana", list[3].Name);
        Assert.Equal("Eve", list[4].Name);
    }

    [Fact]
    public void SqlQuery_OrderByDescending_SortsCorrectly()
    {
        // Arrange
        var data = CreateTestData();

        // Act
        var result = data.ExecuteSql<OrderByTestData, OrderByTestData>(
            "SELECT * FROM data ORDER BY Score DESC");

        // Assert
        var list = result.ToList();
        Assert.Equal(5, list.Count);
        
        // Should be in descending order by Score
        Assert.True(list[0].Score >= list[1].Score);
        Assert.True(list[1].Score >= list[2].Score);
        Assert.True(list[2].Score >= list[3].Score);
        Assert.True(list[3].Score >= list[4].Score);
        
        // Verify highest and lowest
        Assert.Equal(95.0, list[0].Score); // Diana
        Assert.Equal(78.5, list[4].Score); // Bob
    }

    [Fact]
    public void SqlQuery_OrderByMultipleColumns_SortsCorrectly()
    {
        // Arrange
        var data = CreateTestData();

        // Act - Order by Category ASC, then Score DESC within each category
        var result = data.ExecuteSql<OrderByTestData, OrderByTestData>(
            "SELECT * FROM data ORDER BY Category ASC, Score DESC");

        // Assert
        var list = result.ToList();
        Assert.Equal(5, list.Count);
        
        // Group 1: Category A (Alice 92.0, Bob 78.5)
        Assert.Equal("A", list[0].Category);
        Assert.Equal(92.0, list[0].Score); // Alice
        Assert.Equal("A", list[1].Category);
        Assert.Equal(78.5, list[1].Score); // Bob
        
        // Group 2: Category B (Eve 88.0, Charlie 85.5)
        Assert.Equal("B", list[2].Category);
        Assert.Equal(88.0, list[2].Score); // Eve
        Assert.Equal("B", list[3].Category);
        Assert.Equal(85.5, list[3].Score); // Charlie
        
        // Group 3: Category C (Diana 95.0)
        Assert.Equal("C", list[4].Category);
        Assert.Equal(95.0, list[4].Score); // Diana
    }

    [Fact]
    public void SqlQuery_OrderByWithWhere_WorksCorrectly()
    {
        // Arrange
        var data = CreateTestData();

        // Act - Filter then sort
        var result = data.ExecuteSql<OrderByTestData, OrderByTestData>(
            "SELECT * FROM data WHERE Score >= 85 ORDER BY Score ASC");

        // Assert
        var list = result.ToList();
        Assert.Equal(4, list.Count); // 4 records with Score >= 85
        
        // Should be sorted ascending by Score
        Assert.Equal(85.5, list[0].Score); // Charlie
        Assert.Equal(88.0, list[1].Score); // Eve
        Assert.Equal(92.0, list[2].Score); // Alice
        Assert.Equal(95.0, list[3].Score); // Diana
    }

    [Fact]
    public void SqlQuery_OrderByWithLimit_WorksCorrectly()
    {
        // Arrange
        var data = CreateTestData();

        // Act - Sort then limit (top 3)
        var result = data.ExecuteSql<OrderByTestData, OrderByTestData>(
            "SELECT * FROM data ORDER BY Score DESC LIMIT 3");

        // Assert
        var list = result.ToList();
        Assert.Equal(3, list.Count);
        
        // Should be top 3 by Score
        Assert.Equal(95.0, list[0].Score); // Diana
        Assert.Equal(92.0, list[1].Score); // Alice
        Assert.Equal(88.0, list[2].Score); // Eve
    }

    [Fact]
    public void SqlQuery_OrderByInteger_WorksCorrectly()
    {
        // Arrange
        var data = CreateTestData();

        // Act
        var result = data.ExecuteSql<OrderByTestData, OrderByTestData>(
            "SELECT * FROM data ORDER BY Id DESC");

        // Assert
        var list = result.ToList();
        Assert.Equal(5, list.Count);
        
        // Should be descending by Id
        Assert.Equal(5, list[0].Id);
        Assert.Equal(4, list[1].Id);
        Assert.Equal(3, list[2].Id);
        Assert.Equal(2, list[3].Id);
        Assert.Equal(1, list[4].Id);
    }

    [Fact]
    public void SqlParser_OrderByClause_CreatesCorrectPlan()
    {
        // Arrange
        var schema = new Dictionary<string, Type>
        {
            ["Name"] = typeof(string),
            ["Score"] = typeof(double)
        };
        
        var columnIndexMap = new Dictionary<string, int>
        {
            ["Name"] = 0,
            ["Score"] = 1
        };

        var parser = new FrozenArrow.Query.Sql.SqlParser(schema, columnIndexMap, 100);

        // Act
        var sql = "SELECT * FROM data ORDER BY Score DESC, Name ASC";
        var plan = parser.Parse(sql);

        // Assert
        Console.WriteLine($"Plan type: {plan.GetType().Name}");
        Console.WriteLine($"Plan: {plan}");
        
        // The plan should contain a SortPlan somewhere in the tree
        Assert.Contains("Sort", plan.ToString());
    }
}
