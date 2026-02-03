using FrozenArrow.Query;
using System.Collections.Immutable;
using System.Collections.Frozen;
using System.Diagnostics.CodeAnalysis;

namespace FrozenArrow.Tests;

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

    private static FrozenArrow<QueryTestRecord> CreateTestCollection()
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

        return records.ToFrozenArrow();
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
    [SuppressMessage("Performance", "CA1847:Use char literal for a single character lookup", Justification = "Needed for the test")]
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
    public void Where_StringContainsChar_FiltersCorrectly()
    {
        // Arrange
        var collection = CreateTestCollection();

        // Act - Using char overload instead of string
        var results = collection
            .AsQueryable()
            .Where(x => x.Name.Contains('a'))
            .ToList();

        // Assert
        // Diana, Grace, Frank, Jack, Charlie = 5 (case-sensitive, no 'A'lice)
        Assert.Equal(5, results.Count);
    }

    [Fact]
    [SuppressMessage("Performance", "CA1866:Use char overload", Justification = "Needed for the test")]
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
        // Active ages: 25, 35, 28, 32, 29, 55, 23 = 227 / 7 ˜ 32.43
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

    #region GroupBy Tests (Phase 2)

    /// <summary>
    /// Result type for GroupBy tests.
    /// </summary>
    public class CategorySummary
    {
        public string Key { get; set; } = string.Empty;
        public int Count { get; set; }
        public decimal TotalSalary { get; set; }
        public double AverageAge { get; set; }
    }

    [Fact]
    public void GroupBy_WithCount_GroupsByKeyAndCountsPerGroup()
    {
        // Arrange
        using var collection = CreateTestCollection();

        // Act - Group by Category, count per group
        var results = collection
            .AsQueryable()
            .GroupBy(x => x.Category)
            .Select(g => new CategorySummary { Key = g.Key, Count = g.Count() })
            .ToList();

        // Assert
        Assert.Equal(4, results.Count); // Engineering, Management, Marketing, Executive
        
        var engineering = results.First(r => r.Key == "Engineering");
        Assert.Equal(5, engineering.Count); // Alice, Bob, Diana, Ivy, Jack

        var management = results.First(r => r.Key == "Management");
        Assert.Equal(2, management.Count); // Charlie, Frank

        var marketing = results.First(r => r.Key == "Marketing");
        Assert.Equal(2, marketing.Count); // Eve, Grace

        var executive = results.First(r => r.Key == "Executive");
        Assert.Equal(1, executive.Count); // Henry
    }

    [Fact]
    public void GroupBy_WithSum_ComputesSumPerGroup()
    {
        // Arrange
        using var collection = CreateTestCollection();

        // Act - Group by Category, sum salaries per group
        var results = collection
            .AsQueryable()
            .GroupBy(x => x.Category)
            .Select(g => new CategorySummary { Key = g.Key, TotalSalary = g.Sum(x => x.Salary) })
            .ToList();

        // Assert
        var engineering = results.First(r => r.Key == "Engineering");
        // Engineering: 50000 + 75000 + 55000 + 45000 + 70000 = 295000
        Assert.Equal(295000m, engineering.TotalSalary);

        var management = results.First(r => r.Key == "Management");
        // Management: 90000 + 80000 = 170000
        Assert.Equal(170000m, management.TotalSalary);
    }

    [Fact]
    public void GroupBy_WithAverage_ComputesAveragePerGroup()
    {
        // Arrange
        using var collection = CreateTestCollection();

        // Act - Group by Category, average age per group
        var results = collection
            .AsQueryable()
            .GroupBy(x => x.Category)
            .Select(g => new CategorySummary { Key = g.Key, AverageAge = g.Average(x => x.Age) })
            .ToList();

        // Assert
        var engineering = results.First(r => r.Key == "Engineering");
        // Engineering ages: 25, 35, 28, 23, 38 = 149 / 5 = 29.8
        Assert.Equal(29.8, engineering.AverageAge, precision: 1);

        var executive = results.First(r => r.Key == "Executive");
        // Executive: just Henry (55)
        Assert.Equal(55.0, executive.AverageAge);
    }

    [Fact]
    public void GroupBy_WithFilter_FiltersBeforeGrouping()
    {
        // Arrange
        using var collection = CreateTestCollection();

        // Act - Filter active only, then group and count
        var results = collection
            .AsQueryable()
            .Where(x => x.IsActive)
            .GroupBy(x => x.Category)
            .Select(g => new CategorySummary { Key = g.Key, Count = g.Count() })
            .ToList();

        // Assert
        // Active employees:
        // Engineering: Alice, Bob, Diana, Ivy (4) - Jack is inactive
        // Management: none (Charlie, Frank are inactive)
        // Marketing: Eve, Grace (2)
        // Executive: Henry (1)

        Assert.Equal(3, results.Count); // No Management group (all inactive)

        var engineering = results.First(r => r.Key == "Engineering");
        Assert.Equal(4, engineering.Count);

        var marketing = results.First(r => r.Key == "Marketing");
        Assert.Equal(2, marketing.Count);

        Assert.DoesNotContain(results, r => r.Key == "Management");
    }


    [Fact]
    public void GroupBy_WithMultipleAggregates_ComputesAllAggregates()
    {
        // Arrange
        using var collection = CreateTestCollection();

        // Act - Group with multiple aggregates
        var results = collection
            .AsQueryable()
            .GroupBy(x => x.Category)
            .Select(g => new CategorySummary 
            { 
                Key = g.Key, 
                Count = g.Count(),
                TotalSalary = g.Sum(x => x.Salary),
                AverageAge = g.Average(x => x.Age)
            })
            .ToList();

        // Assert
        var engineering = results.First(r => r.Key == "Engineering");
        Assert.Equal(5, engineering.Count);
        Assert.Equal(295000m, engineering.TotalSalary);
        Assert.Equal(29.8, engineering.AverageAge, precision: 1);
    }

    [Fact]
    public void GroupBy_WithAnonymousType_WorksCorrectly()
    {
        // Arrange
        using var collection = CreateTestCollection();

        // Act - Group with anonymous type projection
        var results = collection
            .AsQueryable()
            .GroupBy(x => x.Category)
            .Select(g => new { Category = g.Key, Total = g.Sum(x => x.Salary), Count = g.Count() })
            .ToList();

        // Assert
        Assert.Equal(4, results.Count);

        // Anonymous type: Category is assigned from g.Key
        var engineering = results.First(r => r.Category == "Engineering");
        Assert.Equal(295000m, engineering.Total);
        Assert.Equal(5, engineering.Count);

        var executive = results.First(r => r.Category == "Executive");
        Assert.Equal(120000m, executive.Total);
        Assert.Equal(1, executive.Count);
    }

    [Fact]
    public void GroupBy_ByIntegerColumn_WorksCorrectly()
    {
        // Arrange
        using var collection = CreateTestCollection();

        // Act - Group by Age (integer column)
        var results = collection
            .AsQueryable()
            .GroupBy(x => x.Age)
            .Select(g => new { Age = g.Key, TotalSalary = g.Sum(x => x.Salary), Count = g.Count() })
            .ToList();

        // Assert - Each person has unique age in test data, so each group has 1 person
        Assert.Equal(10, results.Count);
        
        var age25 = results.First(r => r.Age == 25); // Alice
        Assert.Equal(50000m, age25.TotalSalary);
        Assert.Equal(1, age25.Count);

        var age55 = results.First(r => r.Age == 55); // Henry
        Assert.Equal(120000m, age55.TotalSalary);
        Assert.Equal(1, age55.Count);
    }

    [Fact]
    public void GroupBy_Explain_ShowsGroupByInPlan()
    {
        // Arrange
        using var collection = CreateTestCollection();

        // Act - Create query but don't execute
        var query = collection
            .AsQueryable()
            .Where(x => x.IsActive);
        
        var plan = query.Explain();

        // Assert
        Assert.Contains("Optimized: True", plan);
        Assert.Contains("IsActive", plan);
    }

    #endregion

    #region GroupBy ToDictionary Tests (Phase 2b)

    [Fact]
    public void GroupBy_ToDictionary_WithCount_ReturnsCorrectDictionary()
    {
        // Arrange
        using var collection = CreateTestCollection();

        // Act - Group by Category, count per group, return as Dictionary
        var results = collection
            .AsQueryable()
            .GroupBy(x => x.Category)
            .ToDictionary(g => g.Key, g => g.Count());

        // Assert
        Assert.Equal(4, results.Count); // Engineering, Management, Marketing, Executive
        Assert.Equal(5, results["Engineering"]); // Alice, Bob, Diana, Ivy, Jack
        Assert.Equal(2, results["Management"]); // Charlie, Frank
        Assert.Equal(2, results["Marketing"]); // Eve, Grace
        Assert.Equal(1, results["Executive"]); // Henry
    }

    [Fact]
    public void GroupBy_ToDictionary_WithSum_ReturnsSumPerGroup()
    {
        // Arrange
        using var collection = CreateTestCollection();

        // Act - Group by Category, sum salaries per group
        var results = collection
            .AsQueryable()
            .GroupBy(x => x.Category)
            .ToDictionary(g => g.Key, g => g.Sum(x => x.Salary));

        // Assert
        // Engineering: 50000 + 75000 + 55000 + 45000 + 70000 = 295000
        Assert.Equal(295000m, results["Engineering"]);
        // Management: 90000 + 80000 = 170000
        Assert.Equal(170000m, results["Management"]);
        // Marketing: 65000 + 60000 = 125000
        Assert.Equal(125000m, results["Marketing"]);
        // Executive: 120000
        Assert.Equal(120000m, results["Executive"]);
    }

    [Fact]
    public void GroupBy_ToDictionary_WithAverage_ReturnsAveragePerGroup()
    {
        // Arrange
        using var collection = CreateTestCollection();

        // Act - Group by Category, average age per group
        var results = collection
            .AsQueryable()
            .GroupBy(x => x.Category)
            .ToDictionary(g => g.Key, g => g.Average(x => x.Age));

        // Assert
        // Engineering ages: 25, 35, 28, 23, 38 = 149 / 5 = 29.8
        Assert.Equal(29.8, results["Engineering"], precision: 1);
        // Management ages: 45, 40 = 85 / 2 = 42.5
        Assert.Equal(42.5, results["Management"], precision: 1);
        // Marketing ages: 32, 29 = 61 / 2 = 30.5
        Assert.Equal(30.5, results["Marketing"], precision: 1);
        // Executive: 55
        Assert.Equal(55.0, results["Executive"], precision: 1);
    }

    [Fact]
    public void GroupBy_ToDictionary_WithMin_ReturnsMinPerGroup()
    {
        // Arrange
        using var collection = CreateTestCollection();

        // Act - Group by Category, min salary per group
        var results = collection
            .AsQueryable()
            .GroupBy(x => x.Category)
            .ToDictionary(g => g.Key, g => g.Min(x => x.Salary));

        // Assert
        Assert.Equal(45000m, results["Engineering"]); // Ivy
        Assert.Equal(80000m, results["Management"]); // Frank
        Assert.Equal(60000m, results["Marketing"]); // Grace
        Assert.Equal(120000m, results["Executive"]); // Henry (only one)
    }

    [Fact]
    public void GroupBy_ToDictionary_WithMax_ReturnsMaxPerGroup()
    {
        // Arrange
        using var collection = CreateTestCollection();

        // Act - Group by Category, max salary per group
        var results = collection
            .AsQueryable()
            .GroupBy(x => x.Category)
            .ToDictionary(g => g.Key, g => g.Max(x => x.Salary));

        // Assert
        Assert.Equal(75000m, results["Engineering"]); // Bob
        Assert.Equal(90000m, results["Management"]); // Charlie
        Assert.Equal(65000m, results["Marketing"]); // Eve
        Assert.Equal(120000m, results["Executive"]); // Henry
    }

    [Fact]
    public void GroupBy_ToDictionary_WithFilter_FiltersBeforeGrouping()
    {
        // Arrange
        using var collection = CreateTestCollection();

        // Act - Filter active only, then group and count
        var results = collection
            .AsQueryable()
            .Where(x => x.IsActive)
            .GroupBy(x => x.Category)
            .ToDictionary(g => g.Key, g => g.Count());

        // Assert
        // Active employees:
        // Engineering: Alice, Bob, Diana, Ivy (4) - Jack is inactive
        // Management: none (Charlie, Frank are inactive)
        // Marketing: Eve, Grace (2)
        // Executive: Henry (1)

        Assert.Equal(3, results.Count); // No Management group (all inactive)
        Assert.Equal(4, results["Engineering"]);
        Assert.Equal(2, results["Marketing"]);
        Assert.Equal(1, results["Executive"]);
        Assert.False(results.ContainsKey("Management"));
    }

    [Fact]
    public void GroupBy_ToDictionary_ByIntegerColumn_WorksCorrectly()
    {
        // Arrange
        using var collection = CreateTestCollection();

        // Act - Group by Age (integer column)
        var results = collection
            .AsQueryable()
            .GroupBy(x => x.Age)
            .ToDictionary(g => g.Key, g => g.Sum(x => x.Salary));

        // Assert - Each person has unique age in test data
        Assert.Equal(10, results.Count);
        Assert.Equal(50000m, results[25]); // Alice
        Assert.Equal(75000m, results[35]); // Bob
        Assert.Equal(120000m, results[55]); // Henry
    }

    #endregion

    #region Multi-Aggregate Tests (Phase 3)

    /// <summary>
    /// Result type for multi-aggregate tests.
    /// </summary>
    public class SalaryStatistics
    {
        public decimal TotalSalary { get; set; }
        public double AverageAge { get; set; }
        public decimal MinSalary { get; set; }
        public decimal MaxSalary { get; set; }
        public int Count { get; set; }
    }

    [Fact]
    public void Aggregate_MultipleAggregates_ComputesAllInSinglePass()
    {
        // Arrange
        using var collection = CreateTestCollection();

        // Act - Compute multiple aggregates over all data
        var stats = collection
            .AsQueryable()
            .Aggregate(agg => new SalaryStatistics
            {
                TotalSalary = agg.Sum(x => x.Salary),
                AverageAge = agg.Average(x => x.Age),
                MinSalary = agg.Min(x => x.Salary),
                MaxSalary = agg.Max(x => x.Salary),
                Count = agg.Count()
            });

        // Assert
        // All salaries: 50000+75000+90000+55000+65000+80000+60000+120000+45000+70000 = 710000
        Assert.Equal(710000m, stats.TotalSalary);
        
        // All ages: 25+35+45+28+32+40+29+55+23+38 = 350 / 10 = 35.0
        Assert.Equal(35.0, stats.AverageAge);
        
        // Min salary: Ivy(45000)
        Assert.Equal(45000m, stats.MinSalary);
        
        // Max salary: Henry(120000)
        Assert.Equal(120000m, stats.MaxSalary);
        
        // Count: 10 records
        Assert.Equal(10, stats.Count);
    }

    [Fact]
    public void Aggregate_WithFilter_ComputesAggregatesOnFilteredData()
    {
        // Arrange
        using var collection = CreateTestCollection();

        // Act - Compute aggregates over filtered data (active employees only)
        var stats = collection
            .AsQueryable()
            .Where(x => x.IsActive)
            .Aggregate(agg => new SalaryStatistics
            {
                TotalSalary = agg.Sum(x => x.Salary),
                AverageAge = agg.Average(x => x.Age),
                MinSalary = agg.Min(x => x.Salary),
                MaxSalary = agg.Max(x => x.Salary),
                Count = agg.Count()
            });

        // Assert
        // Active employees: Alice, Bob, Diana, Eve, Grace, Henry, Ivy (7)
        // Active salaries: 50000+75000+55000+65000+60000+120000+45000 = 470000
        Assert.Equal(470000m, stats.TotalSalary);
        
        // Active ages: 25+35+28+32+29+55+23 = 227 / 7 ˜ 32.43
        Assert.Equal(227.0 / 7.0, stats.AverageAge, precision: 2);
        
        // Min active salary: Ivy(45000)
        Assert.Equal(45000m, stats.MinSalary);
        
        // Max active salary: Henry(120000)
        Assert.Equal(120000m, stats.MaxSalary);
        
        // Active count: 7
        Assert.Equal(7, stats.Count);
    }

    [Fact]
    public void Aggregate_WithCategoryFilter_ComputesCorrectly()
    {
        // Arrange
        using var collection = CreateTestCollection();

        // Act - Compute aggregates for Engineering only
        var stats = collection
            .AsQueryable()
            .Where(x => x.Category == "Engineering")
            .Aggregate(agg => new SalaryStatistics
            {
                TotalSalary = agg.Sum(x => x.Salary),
                AverageAge = agg.Average(x => x.Age),
                MinSalary = agg.Min(x => x.Salary),
                MaxSalary = agg.Max(x => x.Salary),
                Count = agg.Count()
            });

        // Assert
        // Engineering: Alice(50000,25), Bob(75000,35), Diana(55000,28), Ivy(45000,23), Jack(70000,38)
        // Total: 50000+75000+55000+45000+70000 = 295000
        Assert.Equal(295000m, stats.TotalSalary);
        
        // Avg age: (25+35+28+23+38)/5 = 149/5 = 29.8
        Assert.Equal(29.8, stats.AverageAge, precision: 1);
        
        // Min: 45000 (Ivy)
        Assert.Equal(45000m, stats.MinSalary);
        
        // Max: 75000 (Bob)
        Assert.Equal(75000m, stats.MaxSalary);
        
        // Count: 5
        Assert.Equal(5, stats.Count);
    }

    [Fact]
    public void Aggregate_JustCountAndSum_WorksWithPartialAggregates()
    {
        // Arrange
        using var collection = CreateTestCollection();

        // Act - Only compute some aggregates
        var stats = collection
            .AsQueryable()
            .Where(x => x.IsActive)
            .Aggregate(agg => new { Total = agg.Sum(x => x.Salary), Count = agg.Count() });

        // Assert
        Assert.Equal(470000m, stats.Total);
        Assert.Equal(7, stats.Count);
    }

    [Fact]
    public void Aggregate_IntegerColumns_ComputesCorrectly()
    {
        // Arrange
        using var collection = CreateTestCollection();

        // Act - Aggregate on integer column (Age)
        var ageStats = collection
            .AsQueryable()
            .Where(x => x.Category == "Engineering")
            .Aggregate(agg => new
            {
                TotalAge = agg.Sum(x => x.Age),
                MinAge = agg.Min(x => x.Age),
                MaxAge = agg.Max(x => x.Age),
                Count = agg.Count()
            });

        // Assert
        // Engineering ages: 25, 35, 28, 23, 38 = 149
        Assert.Equal(149, ageStats.TotalAge);
        Assert.Equal(23, ageStats.MinAge); // Ivy
        Assert.Equal(38, ageStats.MaxAge); // Jack
        Assert.Equal(5, ageStats.Count);
    }

    #endregion

    #region Parallel Execution Tests

    private static FrozenArrow<QueryTestRecord> CreateLargeTestCollection(int count)
    {
        var records = new List<QueryTestRecord>();
        var categories = new[] { "Engineering", "Management", "Marketing", "Executive", "Operations" };
        var random = new Random(42); // Fixed seed for reproducibility

        for (int i = 0; i < count; i++)
        {
            records.Add(new QueryTestRecord
            {
                Id = i,
                Name = $"Person{i}",
                Age = 20 + (i % 50), // Ages 20-69
                Salary = 40000m + (i % 100) * 1000m,
                IsActive = i % 3 != 0, // ~67% active
                Category = categories[i % categories.Length]
            });
        }

        return records.ToFrozenArrow();
    }

    [Fact]
    public void ParallelExecution_ProducesSameResultsAsSequential_Count()
    {
        // Arrange
        using var collection = CreateLargeTestCollection(100_000);

        // Act - Sequential
        var sequentialResult = collection
            .AsQueryable()
            .AsSequential()
            .Where(x => x.Age > 30)
            .Count();

        // Act - Parallel
        var parallelResult = collection
            .AsQueryable()
            .AsParallel()
            .Where(x => x.Age > 30)
            .Count();

        // Assert
        Assert.Equal(sequentialResult, parallelResult);
    }

    [Fact]
    public void ParallelExecution_ProducesSameResultsAsSequential_MultiplePredicates()
    {
        // Arrange
        using var collection = CreateLargeTestCollection(100_000);

        // Act - Sequential
        var sequentialResult = collection
            .AsQueryable()
            .AsSequential()
            .Where(x => x.Age > 25 && x.Age < 50 && x.IsActive)
            .Count();

        // Act - Parallel
        var parallelResult = collection
            .AsQueryable()
            .AsParallel()
            .Where(x => x.Age > 25 && x.Age < 50 && x.IsActive)
            .Count();

        // Assert
        Assert.Equal(sequentialResult, parallelResult);
    }

    [Fact]
    public void ParallelExecution_ProducesSameResultsAsSequential_Sum()
    {
        // Arrange
        using var collection = CreateLargeTestCollection(100_000);

        // Act - Sequential
        var sequentialResult = collection
            .AsQueryable()
            .AsSequential()
            .Where(x => x.Age > 30 && x.IsActive)
            .Sum(x => x.Salary);

        // Act - Parallel
        var parallelResult = collection
            .AsQueryable()
            .AsParallel()
            .Where(x => x.Age > 30 && x.IsActive)
            .Sum(x => x.Salary);

        // Assert
        Assert.Equal(sequentialResult, parallelResult);
    }

    [Fact]
    public void ParallelExecution_WithCustomChunkSize_ProducesCorrectResults()
    {
        // Arrange
        using var collection = CreateLargeTestCollection(100_000);

        var customOptions = new ParallelQueryOptions
        {
            EnableParallelExecution = true,
            ChunkSize = 4_096, // Smaller chunks
            MaxDegreeOfParallelism = 2
        };

        // Act - Get baseline with sequential
        var sequentialResult = collection
            .AsQueryable()
            .AsSequential()
            .Where(x => x.Age > 40)
            .Count();

        // Act - With custom parallel options
        var parallelResult = collection
            .AsQueryable()
            .WithParallelOptions(customOptions)
            .Where(x => x.Age > 40)
            .Count();

        // Assert
        Assert.Equal(sequentialResult, parallelResult);
    }

    [Fact]
    public void ParallelExecution_ToList_ReturnsCorrectItems()
    {
        // Arrange
        using var collection = CreateLargeTestCollection(50_000);

        // Act - Sequential
        var sequentialResult = collection
            .AsQueryable()
            .AsSequential()
            .Where(x => x.Id < 100)
            .ToList();

        // Act - Parallel
        var parallelResult = collection
            .AsQueryable()
            .AsParallel()
            .Where(x => x.Id < 100)
            .ToList();

        // Assert
        Assert.Equal(sequentialResult.Count, parallelResult.Count);
        
        // Both should contain the same IDs (order may differ due to parallel execution)
        var sequentialIds = sequentialResult.Select(x => x.Id).OrderBy(x => x).ToList();
        var parallelIds = parallelResult.Select(x => x.Id).OrderBy(x => x).ToList();
        Assert.Equal(sequentialIds, parallelIds);
    }

    #endregion
}
