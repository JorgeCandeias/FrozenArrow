using FrozenArrow.Query;

namespace FrozenArrow.Tests;

/// <summary>
/// Comprehensive tests for Take and Skip pagination operations.
/// </summary>
public class PaginationTests
{
    [ArrowRecord]
    public record PaginationTestRecord
    {
        [ArrowArray(Name = "Id")]
        public int Id { get; init; }

        [ArrowArray(Name = "Value")]
        public int Value { get; init; }

        [ArrowArray(Name = "IsActive")]
        public bool IsActive { get; init; }

        [ArrowArray(Name = "Category")]
        public string Category { get; init; } = string.Empty;
    }

    private static FrozenArrow<PaginationTestRecord> CreateTestData(int count)
    {
        var records = new List<PaginationTestRecord>();
        for (int i = 0; i < count; i++)
        {
            records.Add(new PaginationTestRecord
            {
                Id = i,
                Value = i * 10,
                IsActive = i % 2 == 0,
                Category = i % 3 == 0 ? "A" : (i % 3 == 1 ? "B" : "C")
            });
        }
        return records.ToFrozenArrow();
    }

    #region Take Tests

    [Fact]
    public void Take_ReturnsCorrectCount()
    {
        // Arrange
        var data = CreateTestData(100);

        // Act
        var results = data.AsQueryable()
            .Take(10)
            .ToList();

        // Assert
        Assert.Equal(10, results.Count);
        Assert.Equal(0, results[0].Id);
        Assert.Equal(9, results[9].Id);
    }

    [Fact]
    public void Take_WithFilter_ReturnsCorrectCount()
    {
        // Arrange
        var data = CreateTestData(100);

        // Act
        var results = data.AsQueryable()
            .Where(x => x.IsActive)
            .Take(15)
            .ToList();

        // Assert
        Assert.Equal(15, results.Count);
        Assert.All(results, r => Assert.True(r.IsActive));
    }

    [Fact]
    public void Take_MoreThanAvailable_ReturnsAll()
    {
        // Arrange
        var data = CreateTestData(20);

        // Act
        var results = data.AsQueryable()
            .Take(100)
            .ToList();

        // Assert
        Assert.Equal(20, results.Count);
    }

    [Fact]
    public void Take_Zero_ReturnsEmpty()
    {
        // Arrange
        var data = CreateTestData(100);

        // Act
        var results = data.AsQueryable()
            .Take(0)
            .ToList();

        // Assert
        Assert.Empty(results);
    }

    [Fact]
    public void Take_WithCount_ReturnsCorrectCount()
    {
        // Arrange
        var data = CreateTestData(1000);

        // Act
        var count = data.AsQueryable()
            .Where(x => x.Value > 500)
            .Take(50)
            .Count();

        // Assert
        Assert.Equal(50, count);
    }

    [Fact]
    public void Take_WithLargeDataset_LimitsCorrectly()
    {
        // Arrange - 1 million rows
        var data = CreateTestData(1_000_000);

        // Act
        var results = data.AsQueryable()
            .Where(x => x.IsActive)
            .Take(1000)
            .ToList();

        // Assert
        Assert.Equal(1000, results.Count);
        Assert.All(results, r => Assert.True(r.IsActive));
    }

    #endregion

    #region Skip Tests

    [Fact]
    public void Skip_SkipsCorrectCount()
    {
        // Arrange
        var data = CreateTestData(100);

        // Act
        var results = data.AsQueryable()
            .Skip(10)
            .ToList();

        // Assert
        Assert.Equal(90, results.Count);
        Assert.Equal(10, results[0].Id);
    }

    [Fact]
    public void Skip_WithFilter_SkipsCorrectCount()
    {
        // Arrange
        var data = CreateTestData(100);

        // Act
        var results = data.AsQueryable()
            .Where(x => x.IsActive)
            .Skip(5)
            .ToList();

        // Assert
        // 50 active records (0, 2, 4, ..., 98), skip 5 = 45 remaining
        Assert.Equal(45, results.Count);
        Assert.Equal(10, results[0].Id); // 6th active record (0-indexed)
    }

    [Fact]
    public void Skip_MoreThanAvailable_ReturnsEmpty()
    {
        // Arrange
        var data = CreateTestData(20);

        // Act
        var results = data.AsQueryable()
            .Skip(100)
            .ToList();

        // Assert
        Assert.Empty(results);
    }

    [Fact]
    public void Skip_Zero_ReturnsAll()
    {
        // Arrange
        var data = CreateTestData(100);

        // Act
        var results = data.AsQueryable()
            .Skip(0)
            .ToList();

        // Assert
        Assert.Equal(100, results.Count);
    }

    [Fact]
    public void Skip_WithCount_ReturnsCorrectCount()
    {
        // Arrange
        var data = CreateTestData(1000);

        // Act
        var count = data.AsQueryable()
            .Where(x => x.Value < 5000)
            .Skip(100)
            .Count();

        // Assert - 500 values < 5000, skip 100 = 400
        Assert.Equal(400, count);
    }

    #endregion

    #region Skip + Take Tests (Pagination)

    [Fact]
    public void SkipTake_Pagination_ReturnsCorrectPage()
    {
        // Arrange
        var data = CreateTestData(100);

        // Act - Get page 3 (20 items per page, skip 40)
        var page3 = data.AsQueryable()
            .Skip(40)
            .Take(20)
            .ToList();

        // Assert
        Assert.Equal(20, page3.Count);
        Assert.Equal(40, page3[0].Id);
        Assert.Equal(59, page3[19].Id);
    }

    [Fact]
    public void SkipTake_WithFilter_ReturnsCorrectPage()
    {
        // Arrange
        var data = CreateTestData(200);

        // Act - Get filtered page 2 (10 items per page, skip 10)
        var results = data.AsQueryable()
            .Where(x => x.Category == "A")
            .Skip(10)
            .Take(10)
            .ToList();

        // Assert
        // Category A: 0, 3, 6, 9, ... (every 3rd)
        // Total ~67 records, skip 10, take 10 = 10 records
        Assert.Equal(10, results.Count);
        Assert.All(results, r => Assert.Equal("A", r.Category));
    }

    [Fact]
    public void SkipTake_LastPage_ReturnsRemaining()
    {
        // Arrange
        var data = CreateTestData(95);

        // Act - Get last page (20 items per page, page 5 = skip 80)
        var lastPage = data.AsQueryable()
            .Skip(80)
            .Take(20)
            .ToList();

        // Assert - Only 15 items remaining
        Assert.Equal(15, lastPage.Count);
        Assert.Equal(80, lastPage[0].Id);
        Assert.Equal(94, lastPage[14].Id);
    }

    [Fact]
    public void SkipTake_BeyondData_ReturnsEmpty()
    {
        // Arrange
        var data = CreateTestData(50);

        // Act
        var results = data.AsQueryable()
            .Skip(100)
            .Take(20)
            .ToList();

        // Assert
        Assert.Empty(results);
    }

    [Fact]
    public void SkipTake_WithCount_ReturnsCorrectCount()
    {
        // Arrange
        var data = CreateTestData(500);

        // Act
        var count = data.AsQueryable()
            .Where(x => x.IsActive)
            .Skip(50)
            .Take(100)
            .Count();

        // Assert
        Assert.Equal(100, count);
    }

    [Fact]
    public void SkipTake_MultipleCombinations_WorkCorrectly()
    {
        // Arrange
        var data = CreateTestData(1000);

        // Act & Assert - Page 1
        var page1 = data.AsQueryable().Skip(0).Take(25).ToList();
        Assert.Equal(25, page1.Count);
        Assert.Equal(0, page1[0].Id);

        // Page 2
        var page2 = data.AsQueryable().Skip(25).Take(25).ToList();
        Assert.Equal(25, page2.Count);
        Assert.Equal(25, page2[0].Id);

        // Page 10
        var page10 = data.AsQueryable().Skip(225).Take(25).ToList();
        Assert.Equal(25, page10.Count);
        Assert.Equal(225, page10[0].Id);
    }

    #endregion

    #region Edge Cases

    [Fact]
    public void Take_Negative_ThrowsOrTreatsAsZero()
    {
        // Arrange
        var data = CreateTestData(100);

        // Act & Assert
        // LINQ standard behavior: negative Take throws ArgumentOutOfRangeException
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            data.AsQueryable().Take(-5).ToList());
    }

    [Fact]
    public void Skip_Negative_ThrowsOrTreatsAsZero()
    {
        // Arrange
        var data = CreateTestData(100);

        // Act & Assert
        // LINQ standard behavior: negative Skip throws ArgumentOutOfRangeException
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            data.AsQueryable().Skip(-5).ToList());
    }

    [Fact]
    public void Take_OnEmptyDataset_ReturnsEmpty()
    {
        // Arrange
        var data = CreateTestData(0);

        // Act
        var results = data.AsQueryable()
            .Take(10)
            .ToList();

        // Assert
        Assert.Empty(results);
    }

    [Fact]
    public void Skip_OnEmptyDataset_ReturnsEmpty()
    {
        // Arrange
        var data = CreateTestData(0);

        // Act
        var results = data.AsQueryable()
            .Skip(10)
            .ToList();

        // Assert
        Assert.Empty(results);
    }

    [Fact]
    public void SkipTake_LargeDataset_MemoryEfficient()
    {
        // Arrange - 500K records
        var data = CreateTestData(500_000);

        // Act - Should only materialize 100 records, not all 250K matches
        var results = data.AsQueryable()
            .Where(x => x.IsActive) // ~250K matches
            .Skip(1000)
            .Take(100)
            .ToList();

        // Assert
        Assert.Equal(100, results.Count);
        Assert.All(results, r => Assert.True(r.IsActive));
    }

    #endregion

    #region First/FirstOrDefault with Skip

    [Fact]
    public void First_WithSkip_ReturnsCorrectElement()
    {
        // Arrange
        var data = CreateTestData(100);

        // Act
        var result = data.AsQueryable()
            .Where(x => x.IsActive)
            .Skip(5)
            .First();

        // Assert
        Assert.Equal(10, result.Id); // 6th active element (0-indexed)
    }

    [Fact]
    public void FirstOrDefault_WithSkipBeyondData_ReturnsDefault()
    {
        // Arrange
        var data = CreateTestData(10);

        // Act
        var result = data.AsQueryable()
            .Where(x => x.IsActive)
            .Skip(100)
            .FirstOrDefault();

        // Assert
        Assert.Null(result);
    }

    #endregion

    #region Any/Count with Pagination

    [Fact]
    public void Any_WithTake_ChecksOnlyTakenElements()
    {
        // Arrange
        var data = CreateTestData(1000);

        // Act
        var hasActive = data.AsQueryable()
            .Take(10)
            .Any(x => x.IsActive);

        // Assert
        Assert.True(hasActive); // First 10 include even numbers (active)
    }

    [Fact]
    public void Count_WithTake_CountsOnlyTakenElements()
    {
        // Arrange
        var data = CreateTestData(1000);

        // Act - .Take(20).Count(predicate) should take first 20, then count matches
        var activeCount = data.AsQueryable()
            .Take(20)
            .Count(x => x.IsActive);

        // Assert - First 20 elements (IDs 0-19), even IDs are active (0,2,4...18) = 10
        Assert.Equal(10, activeCount);
    }

    [Fact]
    public void LongCount_WithSkipTake_ReturnsCorrectCount()
    {
        // Arrange
        var data = CreateTestData(10_000);

        // Act
        var count = data.AsQueryable()
            .Where(x => x.Value > 1000)
            .Skip(500)
            .Take(1000)
            .LongCount();

        // Assert
        Assert.Equal(1000L, count);
    }

    #endregion

    #region Operation Order Tests (Regression Prevention)

    [Fact]
    public void OperationOrder_TakeThenWhere_AppliesToLimitedSet()
    {
        // Arrange
        var data = CreateTestData(1000);

        // Act - Take 100, then filter within those 100
        var results = data.AsQueryable()
            .Take(100)
            .Where(x => x.IsActive)
            .ToList();

        // Assert - Should only consider first 100 rows (IDs 0-99)
        // Even IDs in first 100 = 50 items
        Assert.Equal(50, results.Count);
        Assert.All(results, r => Assert.True(r.Id < 100 && r.IsActive));
    }

    [Fact]
    public void OperationOrder_WhereThenTake_FiltersAllThenLimits()
    {
        // Arrange
        var data = CreateTestData(1000);

        // Act - Filter all, then take 100 from filtered
        var results = data.AsQueryable()
            .Where(x => x.IsActive)
            .Take(100)
            .ToList();

        // Assert - Should filter all 1000, then take first 100 of those
        // All active records, limited to 100
        Assert.Equal(100, results.Count);
        Assert.All(results, r => Assert.True(r.IsActive));
        // First 100 active records are IDs 0,2,4,6...198
        Assert.Equal(0, results[0].Id);
        Assert.Equal(198, results[99].Id);
    }

    [Fact]
    public void OperationOrder_TakeThenWhereWithCount_CountsCorrectly()
    {
        // Arrange
        var data = CreateTestData(1000);

        // Act - Take 50, then count how many match predicate
        var count1 = data.AsQueryable()
            .Take(50)
            .Count(x => x.Value > 200);

        // Compare to explicit Where
        var count2 = data.AsQueryable()
            .Take(50)
            .Where(x => x.Value > 200)
            .Count();

        // Assert - Both should give same result
        // First 50 items have Values 0,10,20...490
        // Values > 200: 210,220,...490 = 29 items
        Assert.Equal(29, count1);
        Assert.Equal(29, count2);
    }

    [Fact]
    public void OperationOrder_WhereThenTakeWithCount_CountsCorrectly()
    {
        // Arrange
        var data = CreateTestData(1000);

        // Act - Filter all, then take 50, then count
        var count = data.AsQueryable()
            .Where(x => x.Value > 200)
            .Take(50)
            .Count();

        // Assert - Filter finds many, take limits to 50, count = 50
        Assert.Equal(50, count);
    }

    [Fact]
    public void OperationOrder_ComplexChain_TakeWhereTakeCount()
    {
        // Arrange
        var data = CreateTestData(1000);

        // Act - Take 200, filter those, take 10 of filtered, count
        var count = data.AsQueryable()
            .Take(200)
            .Where(x => x.Category == "A")
            .Take(10)
            .Count();

        // Assert - First Take limits to 200, Where filters those, second Take limits to 10
        Assert.Equal(10, count);
    }

    [Fact]
    public void OperationOrder_SkipThenWhere_AppliesToSkippedSet()
    {
        // Arrange
        var data = CreateTestData(1000);

        // Act - Skip 100, then filter the remaining
        var results = data.AsQueryable()
            .Skip(100)
            .Where(x => x.IsActive)
            .ToList();

        // Assert - Should skip first 100 (IDs 0-99), then filter remaining (IDs 100-999)
        // Active records from 100-999 = IDs 100,102,104...998 = 450 items
        Assert.Equal(450, results.Count);
        Assert.All(results, r => Assert.True(r.Id >= 100 && r.IsActive));
    }

    [Fact]
    public void OperationOrder_WhereTheSkip_FiltersAllThenSkips()
    {
        // Arrange
        var data = CreateTestData(1000);

        // Act - Filter all, then skip 100 from filtered
        var results = data.AsQueryable()
            .Where(x => x.IsActive)
            .Skip(100)
            .ToList();

        // Assert - 500 active total, skip 100, leaves 400
        Assert.Equal(400, results.Count);
        Assert.All(results, r => Assert.True(r.IsActive));
        // Should start at 101st active record (ID 200)
        Assert.Equal(200, results[0].Id);
    }

    [Fact]
    public void OperationOrder_MultipleWhereClauses_AllAppliedCorrectly()
    {
        // Arrange
        var data = CreateTestData(1000);

        // Act - Multiple Where clauses with Take in between
        var results = data.AsQueryable()
            .Where(x => x.Value > 100)
            .Take(200)
            .Where(x => x.IsActive)
            .ToList();

        // Assert - First Where filters, Take limits, second Where filters again
        var expectedCount = results.Count;
        Assert.All(results, r =>
        {
            Assert.True(r.Value > 100);
            Assert.True(r.IsActive);
        });
        // Should have taken 200 items where Value > 100, then filtered for IsActive
        Assert.True(expectedCount <= 200);
    }

    #endregion
}
