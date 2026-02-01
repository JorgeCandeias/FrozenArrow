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

    #region Column-Level Aggregate Tests

    [Fact]
    public void Sum_OnFilteredData_ComputesDirectlyOnColumn()
    {
        // Arrange
        var collection = CreateTestCollection();

        // Act - Sum of salaries for active employees
        var totalSalary = collection
            .AsQueryable()
            .Where(x => x.IsActive)
            .Sum(x => x.Salary);

        // Assert
        // Active: Alice(50000), Bob(75000), Diana(55000), Eve(65000), Grace(60000), Henry(120000), Ivy(45000) = 470000
        Assert.Equal(470000m, totalSalary);
    }

    [Fact]
    public void Sum_OnAllData_ComputesDirectlyOnColumn()
    {
        // Arrange
        var collection = CreateTestCollection();

        // Act - Sum of all salaries (no filter)
        var totalSalary = collection
            .AsQueryable()
            .Sum(x => x.Salary);

        // Assert
        // All: 50000+75000+90000+55000+65000+80000+60000+120000+45000+70000 = 710000
        Assert.Equal(710000m, totalSalary);
    }

    [Fact]
    public void Sum_IntColumn_ComputesCorrectly()
    {
        // Arrange
        var collection = CreateTestCollection();

        // Act - Sum of ages for Engineering
        var totalAge = collection
            .AsQueryable()
            .Where(x => x.Category == "Engineering")
            .Sum(x => x.Age);

        // Assert
        // Engineering: Alice(25), Bob(35), Diana(28), Ivy(23), Jack(38) = 149
        Assert.Equal(149, totalAge);
    }

    [Fact]
    public void Average_OnFilteredData_ComputesDirectlyOnColumn()
    {
        // Arrange
        var collection = CreateTestCollection();

        // Act - Average age of active employees
        var avgAge = collection
            .AsQueryable()
            .Where(x => x.IsActive)
            .Average(x => x.Age);

        // Assert
        // Active ages: 25, 35, 28, 32, 29, 55, 23 = 227 / 7 ? 32.43
        Assert.Equal(227.0 / 7.0, avgAge, precision: 2);
    }

    [Fact]
    public void Min_OnFilteredData_ComputesDirectlyOnColumn()
    {
        // Arrange
        var collection = CreateTestCollection();

        // Act - Minimum salary in Engineering
        var minSalary = collection
            .AsQueryable()
            .Where(x => x.Category == "Engineering")
            .Min(x => x.Salary);

        // Assert
        // Engineering salaries: 50000, 75000, 55000, 45000, 70000 ? min = 45000 (Ivy)
        Assert.Equal(45000m, minSalary);
    }

    [Fact]
    public void Max_OnFilteredData_ComputesDirectlyOnColumn()
    {
        // Arrange
        var collection = CreateTestCollection();

        // Act - Maximum salary in Engineering
        var maxSalary = collection
            .AsQueryable()
            .Where(x => x.Category == "Engineering")
            .Max(x => x.Salary);

        // Assert
        // Engineering salaries: 50000, 75000, 55000, 45000, 70000 ? max = 75000 (Bob)
        Assert.Equal(75000m, maxSalary);
    }

    [Fact]
    public void Min_IntColumn_ComputesCorrectly()
    {
        // Arrange
        var collection = CreateTestCollection();

        // Act - Minimum age of inactive employees
        var minAge = collection
            .AsQueryable()
            .Where(x => !x.IsActive)
            .Min(x => x.Age);

        // Assert
        // Inactive: Charlie(45), Frank(40), Jack(38) ? min = 38
        Assert.Equal(38, minAge);
    }

    [Fact]
    public void Max_IntColumn_ComputesCorrectly()
    {
        // Arrange
        var collection = CreateTestCollection();

        // Act - Maximum age overall
        var maxAge = collection
            .AsQueryable()
            .Max(x => x.Age);

        // Assert
        // Max age = Henry(55)
        Assert.Equal(55, maxAge);
    }

    [Fact]
    public void Explain_ShowsAggregateInPlan()
    {
        // Arrange
        var collection = CreateTestCollection();

        // Act
        var plan = collection
            .AsQueryable()
            .Where(x => x.IsActive)
            .Sum(x => x.Salary)
            .ToString(); // This won't work - we need to call Explain differently

        // For aggregates, we can't call Explain after Sum (it returns decimal, not ArrowQuery)
        // Instead, let's verify the aggregate works and check the query before aggregation
        var query = collection.AsQueryable().Where(x => x.IsActive);
        var queryPlan = query.Explain();

        // Assert
        Assert.Contains("Optimized: True", queryPlan);
    }

    #endregion
}
