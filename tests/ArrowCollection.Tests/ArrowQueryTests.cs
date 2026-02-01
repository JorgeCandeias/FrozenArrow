using ArrowCollection.Query;
using System.Collections.Immutable;
using System.Collections.Frozen;

namespace ArrowCollection.Tests;

/// <summary>
/// Tests for ArrowQuery LINQ functionality.
/// </summary>
public class ArrowQueryTests
{
    [ArrowRecord]
    public record QueryTestRecord
    {
        [ArrowArray(Name = "Id")]
        public int Id { get; init; }

        [ArrowArray(Name = "Name")]
        public string Name { get; init; } = string.Empty;

        [ArrowArray(Name = "Age")]
        public int Age { get; init; }

        [ArrowArray(Name = "Salary")]
        public decimal Salary { get; init; }

        [ArrowArray(Name = "IsActive")]
        public bool IsActive { get; init; }

        [ArrowArray(Name = "Category")]
        public string Category { get; init; } = string.Empty;
    }

    private static ArrowCollection<QueryTestRecord> CreateTestCollection()
    {
        var records = new List<QueryTestRecord>
        {
            new() { Id = 1, Name = "Alice", Age = 25, Salary = 50000m, IsActive = true, Category = "Engineering" },
            new() { Id = 2, Name = "Bob", Age = 35, Salary = 75000m, IsActive = true, Category = "Engineering" },
            new() { Id = 3, Name = "Charlie", Age = 45, Salary = 90000m, IsActive = false, Category = "Management" },
            new() { Id = 4, Name = "Diana", Age = 28, Salary = 55000m, IsActive = true, Category = "Engineering" },
            new() { Id = 5, Name = "Eve", Age = 32, Salary = 65000m, IsActive = true, Category = "Marketing" },
            new() { Id = 6, Name = "Frank", Age = 40, Salary = 80000m, IsActive = false, Category = "Management" },
            new() { Id = 7, Name = "Grace", Age = 29, Salary = 60000m, IsActive = true, Category = "Marketing" },
            new() { Id = 8, Name = "Henry", Age = 55, Salary = 120000m, IsActive = true, Category = "Executive" },
            new() { Id = 9, Name = "Ivy", Age = 23, Salary = 45000m, IsActive = true, Category = "Engineering" },
            new() { Id = 10, Name = "Jack", Age = 38, Salary = 70000m, IsActive = false, Category = "Engineering" },
        };

        return records.ToArrowCollection();
    }

    [Fact]
    public void AsQueryable_ReturnsArrowQuery()
    {
        // Arrange
        var collection = CreateTestCollection();

        // Act
        var query = collection.AsQueryable();

        // Assert
        Assert.IsType<ArrowQuery<QueryTestRecord>>(query);
    }

    [Fact]
    public void Where_SimpleIntComparison_FiltersCorrectly()
    {
        // Arrange
        var collection = CreateTestCollection();

        // Act
        var results = collection
            .AsQueryable()
            .Where(x => x.Age > 30)
            .ToList();

        // Assert
        // Bob(35), Charlie(45), Eve(32), Frank(40), Henry(55), Jack(38) = 6 people
        Assert.Equal(6, results.Count);
        Assert.All(results, r => Assert.True(r.Age > 30));
    }

    [Fact]
    public void Where_IntEquality_FiltersCorrectly()
    {
        // Arrange
        var collection = CreateTestCollection();

        // Act
        var results = collection
            .AsQueryable()
            .Where(x => x.Id == 5)
            .ToList();

        // Assert
        Assert.Single(results);
        Assert.Equal("Eve", results[0].Name);
    }

    [Fact]
    public void Where_StringEquality_FiltersCorrectly()
    {
        // Arrange
        var collection = CreateTestCollection();

        // Act
        var results = collection
            .AsQueryable()
            .Where(x => x.Category == "Engineering")
            .ToList();

        // Assert
        Assert.Equal(5, results.Count);
        Assert.All(results, r => Assert.Equal("Engineering", r.Category));
    }

    [Fact]
    public void Where_BooleanProperty_FiltersCorrectly()
    {
        // Arrange
        var collection = CreateTestCollection();

        // Act
        var results = collection
            .AsQueryable()
            .Where(x => x.IsActive)
            .ToList();

        // Assert
        Assert.Equal(7, results.Count);
        Assert.All(results, r => Assert.True(r.IsActive));
    }

    [Fact]
    public void Where_NegatedBoolean_FiltersCorrectly()
    {
        // Arrange
        var collection = CreateTestCollection();

        // Act
        var results = collection
            .AsQueryable()
            .Where(x => !x.IsActive)
            .ToList();

        // Assert
        Assert.Equal(3, results.Count);
        Assert.All(results, r => Assert.False(r.IsActive));
    }

    [Fact]
    public void Where_CombinedWithAnd_FiltersCorrectly()
    {
        // Arrange
        var collection = CreateTestCollection();

        // Act
        var results = collection
            .AsQueryable()
            .Where(x => x.Age > 30 && x.IsActive)
            .ToList();

        // Assert
        Assert.Equal(3, results.Count);
        Assert.All(results, r =>
        {
            Assert.True(r.Age > 30);
            Assert.True(r.IsActive);
        });
    }

    [Fact]
    public void Where_MultipleWhereClauses_CombinesCorrectly()
    {
        // Arrange
        var collection = CreateTestCollection();

        // Act
        var results = collection
            .AsQueryable()
            .Where(x => x.Age > 25)
            .Where(x => x.Category == "Engineering")
            .ToList();

        // Assert
        Assert.Equal(3, results.Count);
        Assert.All(results, r =>
        {
            Assert.True(r.Age > 25);
            Assert.Equal("Engineering", r.Category);
        });
    }

    [Fact]
    public void Where_StringContains_FiltersCorrectly()
    {
        // Arrange
        var collection = CreateTestCollection();

        // Act
        var results = collection
            .AsQueryable()
            .Where(x => x.Name.Contains("a"))
            .ToList();

        // Assert
        // Diana, Grace, Frank, Jack, Charlie = 5 (case-sensitive, no 'A'lice)
        Assert.Equal(5, results.Count);
    }

    [Fact]
    public void Where_StringStartsWith_FiltersCorrectly()
    {
        // Arrange
        var collection = CreateTestCollection();

        // Act
        var results = collection
            .AsQueryable()
            .Where(x => x.Name.StartsWith("J"))
            .ToList();

        // Assert
        Assert.Single(results);
        Assert.Equal("Jack", results[0].Name);
    }

    [Fact]
    public void Count_WithPredicate_ReturnsCorrectCount()
    {
        // Arrange
        var collection = CreateTestCollection();

        // Act
        var count = collection
            .AsQueryable()
            .Where(x => x.Age > 30)
            .Count();

        // Assert
        // Bob(35), Charlie(45), Eve(32), Frank(40), Henry(55), Jack(38) = 6 people
        Assert.Equal(6, count);
    }

    [Fact]
    public void Any_WithMatchingPredicate_ReturnsTrue()
    {
        // Arrange
        var collection = CreateTestCollection();

        // Act
        var hasExecutive = collection
            .AsQueryable()
            .Where(x => x.Category == "Executive")
            .Any();

        // Assert
        Assert.True(hasExecutive);
    }

    [Fact]
    public void Any_WithNoMatchingPredicate_ReturnsFalse()
    {
        // Arrange
        var collection = CreateTestCollection();

        // Act
        var hasIntern = collection
            .AsQueryable()
            .Where(x => x.Category == "Intern")
            .Any();

        // Assert
        Assert.False(hasIntern);
    }

    [Fact]
    public void First_WithMatchingPredicate_ReturnsFirstMatch()
    {
        // Arrange
        var collection = CreateTestCollection();

        // Act
        var result = collection
            .AsQueryable()
            .Where(x => x.Age > 40)
            .First();

        // Assert
        Assert.True(result.Age > 40);
    }

    [Fact]
    public void Explain_ReturnsQueryPlan()
    {
        // Arrange
        var collection = CreateTestCollection();

        // Act
        var plan = collection
            .AsQueryable()
            .Where(x => x.Age > 30 && x.IsActive)
            .Explain();

        // Assert
        Assert.Contains("Query Plan", plan);
        Assert.Contains("Optimized", plan);
    }

    [Fact]
    public void Where_WithCapturedVariable_FiltersCorrectly()
    {
        // Arrange
        var collection = CreateTestCollection();
        var minAge = 30;
        var category = "Engineering";

        // Act
        var results = collection
            .AsQueryable()
            .Where(x => x.Age > minAge && x.Category == category)
            .ToList();

        // Assert
        Assert.Equal(2, results.Count); // Bob and Jack
    }

    [Fact]
    public void Where_DecimalComparison_FiltersCorrectly()
    {
        // Arrange
        var collection = CreateTestCollection();

        // Act
        var results = collection
            .AsQueryable()
            .Where(x => x.Salary >= 70000m)
            .ToList();

        // Assert
        Assert.Equal(5, results.Count);
        Assert.All(results, r => Assert.True(r.Salary >= 70000m));
    }

    [Fact]
    public void Query_ReturnsArrowQueryWithExplainAccess()
    {
        // Arrange
        var collection = CreateTestCollection();

        // Act
        var query = collection.Query();
        var plan = query.Explain();

        // Assert
        Assert.IsType<ArrowQuery<QueryTestRecord>>(query);
        Assert.NotNull(plan);
    }

    [Fact]
    public void AllowFallback_DisablesStrictMode()
    {
        // Arrange
        var collection = CreateTestCollection();

        // Act & Assert - should not throw even with complex predicate
        var query = collection
            .AsQueryable()
            .AllowFallback();

        Assert.NotNull(query);
    }

    [Fact]
    public void ToImmutableArray_WorksWithArrowQuery()
    {
        // Arrange
        var collection = CreateTestCollection();

        // Act - ToImmutableArray is from System.Collections.Immutable
        var results = collection
            .AsQueryable()
            .Where(x => x.Age > 30)
            .ToImmutableArray();

        // Assert
        Assert.IsType<ImmutableArray<QueryTestRecord>>(results);
        Assert.Equal(6, results.Length);
        Assert.All(results, r => Assert.True(r.Age > 30));
    }

    [Fact]
    public void ToFrozenSet_WorksWithArrowQuery()
    {
        // Arrange
        var collection = CreateTestCollection();

        // Act - ToFrozenSet is from System.Collections.Frozen
        var results = collection
            .AsQueryable()
            .Where(x => x.Category == "Engineering")
            .ToFrozenSet();

        // Assert
        Assert.Equal(5, results.Count);
        Assert.All(results, r => Assert.Equal("Engineering", r.Category));
    }

    [Fact]
    public void ToHashSet_WorksWithArrowQuery()
    {
        // Arrange
        var collection = CreateTestCollection();

        // Act
        var results = collection
            .AsQueryable()
            .Where(x => x.IsActive)
            .ToHashSet();

        // Assert
        Assert.Equal(7, results.Count);
        Assert.All(results, r => Assert.True(r.IsActive));
    }

    [Fact]
    public void ToDictionary_WorksWithArrowQuery()
    {
        // Arrange
        var collection = CreateTestCollection();

        // Act
        var results = collection
            .AsQueryable()
            .Where(x => x.Id <= 3)
            .ToDictionary(x => x.Id);

        // Assert
        Assert.Equal(3, results.Count);
        Assert.Contains(1, results.Keys);
        Assert.Contains(2, results.Keys);
        Assert.Contains(3, results.Keys);
    }
}
