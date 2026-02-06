using FrozenArrow.Query;

namespace FrozenArrow.Tests.Advanced;

/// <summary>
/// Tests for edge cases and boundary conditions across all operations.
/// Ensures robust handling of empty data, nulls, extreme values, and corner cases.
/// </summary>
public class EdgeCaseTests
{
    [ArrowRecord]
    public record EdgeCaseRecord
    {
        [ArrowArray(Name = "Id")]
        public int Id { get; init; }

        [ArrowArray(Name = "Value")]
        public int Value { get; init; }

        [ArrowArray(Name = "Score")]
        public double Score { get; init; }
    }

    [Fact]
    public void EdgeCase_EmptyDataset_AllOperationsHandled()
    {
        // Arrange
        var data = new List<EdgeCaseRecord>().ToFrozenArrow();

        // Act & Assert - All operations should handle empty data gracefully
        Assert.Equal(0, data.AsQueryable().AllowFallback().Count());
        Assert.False(data.AsQueryable().AllowFallback().Any());
        Assert.Empty(data.AsQueryable().AllowFallback().ToList());
        Assert.Throws<InvalidOperationException>(() => data.AsQueryable().AllowFallback().First());
        Assert.Null(data.AsQueryable().AllowFallback().FirstOrDefault());
        Assert.Equal(0, data.AsQueryable().AllowFallback().Sum(x => x.Value));
        
        // Test Average on empty - should throw
        var avgResult = Assert.Throws<InvalidOperationException>(() => data.AsQueryable().AllowFallback().Average(x => x.Score));
        Assert.Contains("no elements", avgResult.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void EdgeCase_SingleElement_AllOperationsCorrect()
    {
        // Arrange
        var data = new List<EdgeCaseRecord>
        {
            new() { Id = 42, Value = 100, Score = 50.0 }
        }.ToFrozenArrow();

        // Act & Assert
        Assert.Equal(1, data.AsQueryable().AllowFallback().Count());
        Assert.True(data.AsQueryable().AllowFallback().Any());
        Assert.Single(data.AsQueryable().AllowFallback().ToList());
        Assert.Equal(42, data.AsQueryable().AllowFallback().First().Id);
        Assert.Equal(42, data.AsQueryable().AllowFallback().FirstOrDefault()?.Id);
        Assert.Equal(100, data.AsQueryable().AllowFallback().Sum(x => x.Value));
        Assert.Equal(50.0, data.AsQueryable().AllowFallback().Average(x => x.Score));
    }

    [Theory]
    [InlineData(int.MinValue)]
    [InlineData(int.MaxValue)]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(1)]
    public void EdgeCase_ExtremeIntValues_HandledCorrectly(int extremeValue)
    {
        // Arrange
        var data = new List<EdgeCaseRecord>
        {
            new() { Id = 0, Value = extremeValue, Score = 0.0 },
            new() { Id = 1, Value = 0, Score = 0.0 },
            new() { Id = 2, Value = extremeValue, Score = 0.0 }
        }.ToFrozenArrow();

        // Act
        var count = data.AsQueryable().AllowFallback().Where(x => x.Value == extremeValue).Count();
        
        // Assert
        // When extremeValue is 0, all three records match (since record 1 also has Value=0)
        int expectedCount = extremeValue == 0 ? 3 : 2;
        Assert.Equal(expectedCount, count);
        
        if (extremeValue == int.MaxValue)
        {
            // Can't sum two int.MaxValue without overflow, but should handle single value
            var single = data.AsQueryable().AllowFallback().Where(x => x.Id == 0).Sum(x => x.Value);
            Assert.Equal(extremeValue, single);
        }
        else if (extremeValue == int.MinValue)
        {
            // Similar issue with int.MinValue
            var single = data.AsQueryable().AllowFallback().Where(x => x.Id == 0).Sum(x => x.Value);
            Assert.Equal(extremeValue, single);
        }
        else
        {
            // For other values, sum should work
            var sum = data.AsQueryable().AllowFallback().Where(x => x.Value == extremeValue).Sum(x => x.Value);
            Assert.Equal(extremeValue * expectedCount, sum);
        }
    }

    [Theory]
    [InlineData(double.MinValue)]
    [InlineData(double.MaxValue)]
    [InlineData(double.Epsilon)]
    [InlineData(double.NegativeInfinity)]
    [InlineData(double.PositiveInfinity)]
    [InlineData(0.0)]
    public void EdgeCase_ExtremeDoubleValues_HandledCorrectly(double extremeValue)
    {
        // Arrange
        var data = new List<EdgeCaseRecord>
        {
            new() { Id = 0, Value = 0, Score = extremeValue },
            new() { Id = 1, Value = 0, Score = 0.0 },
            new() { Id = 2, Value = 0, Score = extremeValue }
        }.ToFrozenArrow();

        // Act
        var count = data.AsQueryable().AllowFallback().Where(x => x.Score == extremeValue).Count();

        // Assert
        // When extremeValue is 0, all three records match (since record 1 also has Score=0.0)
        int expectedCount = extremeValue == 0.0 ? 3 : 2;
        Assert.Equal(expectedCount, count);
    }

    [Fact]
    public void EdgeCase_AllRecordsMatchPredicate_HandledCorrectly()
    {
        // Arrange
        var data = Enumerable.Range(0, 1000)
            .Select(i => new EdgeCaseRecord { Id = i, Value = 100, Score = 50.0 })
            .ToList()
            .ToFrozenArrow();

        // Act
        var count = data.AsQueryable().AllowFallback().Where(x => x.Value == 100).Count();
        var list = data.AsQueryable().AllowFallback().Where(x => x.Value == 100).ToList();

        // Assert
        Assert.Equal(1000, count);
        Assert.Equal(1000, list.Count);
    }

    [Fact]
    public void EdgeCase_NoRecordsMatchPredicate_HandledCorrectly()
    {
        // Arrange
        var data = Enumerable.Range(0, 1000)
            .Select(i => new EdgeCaseRecord { Id = i, Value = i, Score = i / 10.0 })
            .ToList()
            .ToFrozenArrow();

        // Act
        var count = data.AsQueryable().AllowFallback().Where(x => x.Value > 10000).Count();
        var list = data.AsQueryable().AllowFallback().Where(x => x.Value > 10000).ToList();
        var any = data.AsQueryable().AllowFallback().Where(x => x.Value > 10000).Any();

        // Assert
        Assert.Equal(0, count);
        Assert.Empty(list);
        Assert.False(any);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(10)]
    [InlineData(100)]
    public void EdgeCase_VerySmallDatasets_HandledCorrectly(int size)
    {
        // Arrange
        var data = Enumerable.Range(0, size)
            .Select(i => new EdgeCaseRecord { Id = i, Value = i, Score = i / 10.0 })
            .ToList()
            .ToFrozenArrow();

        // Act
        var count = data.AsQueryable().AllowFallback().Count();
        var filtered = data.AsQueryable().AllowFallback().Where(x => x.Value > size / 2).Count();
        var sum = data.AsQueryable().AllowFallback().Sum(x => x.Value);

        // Assert
        Assert.Equal(size, count);
        Assert.True(filtered >= 0 && filtered <= size);
        Assert.Equal(Enumerable.Range(0, size).Sum(), sum);
    }

    [Fact]
    public void EdgeCase_ConsecutiveFiltersAllEmpty_HandledCorrectly()
    {
        // Arrange
        var data = Enumerable.Range(0, 1000)
            .Select(i => new EdgeCaseRecord { Id = i, Value = i, Score = i / 10.0 })
            .ToList()
            .ToFrozenArrow();

        // Act - Each filter eliminates all remaining data
        var count = data.AsQueryable().AllowFallback()
            .Where(x => x.Value > 10000)  // No matches
            .Where(x => x.Value < 5000)   // Would match, but no data left
            .Where(x => x.Score > 0.0)    // Would match, but no data left
            .Count();

        // Assert
        Assert.Equal(0, count);
    }

    [Fact]
    public void EdgeCase_AlternatingPredicates_SparseResults()
    {
        // Arrange - Create data where every other record matches
        var data = Enumerable.Range(0, 10000)
            .Select(i => new EdgeCaseRecord { Id = i, Value = i % 2, Score = i / 10.0 })
            .ToList()
            .ToFrozenArrow();

        // Act
        var evenCount = data.AsQueryable().AllowFallback().Where(x => x.Value == 0).Count();
        var oddCount = data.AsQueryable().AllowFallback().Where(x => x.Value == 1).Count();

        // Assert
        Assert.Equal(5000, evenCount);
        Assert.Equal(5000, oddCount);
        Assert.Equal(10000, evenCount + oddCount);
    }

    [Theory]
    [InlineData(1)]      // Every record matches
    [InlineData(2)]      // Every other
    [InlineData(10)]     // Every 10th
    [InlineData(100)]    // Every 100th
    [InlineData(1000)]   // Every 1000th
    public void EdgeCase_VariableSelectivity_HandledCorrectly(int frequency)
    {
        // Test queries with different selectivities
        
        // Arrange
        var rowCount = 10000;
        var data = Enumerable.Range(0, rowCount)
            .Select(i => new EdgeCaseRecord { Id = i, Value = i, Score = i / 10.0 })
            .ToList()
            .ToFrozenArrow();

        // Note: This test uses modulo operator which requires fallback materialization
        // Enable fallback mode to allow complex expressions
        
        // Act
        var count = data.AsQueryable().AllowFallback().AllowFallback().Where(x => x.Id % frequency == 0).Count();

        var expectedCount = rowCount / frequency + (rowCount % frequency == 0 ? 0 : 1);

        // Assert
        Assert.Equal(expectedCount, count);
    }

    [Fact]
    public void EdgeCase_DuplicateValues_HandledCorrectly()
    {
        // Arrange - All records have same value
        var data = Enumerable.Range(0, 1000)
            .Select(i => new EdgeCaseRecord { Id = i, Value = 42, Score = 3.14 })
            .ToList()
            .ToFrozenArrow();

        // Act
        var count = data.AsQueryable().AllowFallback().Where(x => x.Value == 42).Count();
        var sum = data.AsQueryable().AllowFallback().Sum(x => x.Value);
        var avg = data.AsQueryable().AllowFallback().Average(x => x.Score);

        // Assert
        Assert.Equal(1000, count);
        Assert.Equal(42000, sum);
        Assert.Equal(3.14, avg, precision: 10);
    }

    [Fact]
    public void EdgeCase_ZeroValues_HandledCorrectly()
    {
        // Arrange
        var data = new List<EdgeCaseRecord>
        {
            new() { Id = 0, Value = 0, Score = 0.0 },
            new() { Id = 1, Value = 0, Score = 0.0 },
            new() { Id = 2, Value = 0, Score = 0.0 }
        }.ToFrozenArrow();

        // Act
        var sum = data.AsQueryable().AllowFallback().Sum(x => x.Value);
        var avg = data.AsQueryable().AllowFallback().Average(x => x.Score);
        var count = data.AsQueryable().AllowFallback().Where(x => x.Value == 0).Count();

        // Assert
        Assert.Equal(0, sum);
        Assert.Equal(0.0, avg);
        Assert.Equal(3, count);
    }

    [Fact]
    public void EdgeCase_FirstAndLastElements_AccessibleCorrectly()
    {
        // Arrange
        var data = Enumerable.Range(0, 10000)
            .Select(i => new EdgeCaseRecord { Id = i, Value = i, Score = i / 10.0 })
            .ToList()
            .ToFrozenArrow();

        // Act
        var first = data.AsQueryable().AllowFallback().OrderBy(x => x.Id).First();
        var last = data.AsQueryable().AllowFallback().OrderBy(x => x.Id).ToList().Last();

        // Assert
        Assert.Equal(0, first.Id);
        Assert.Equal(9999, last.Id);
    }

    [Theory]
    [InlineData(16383)]  // Just before chunk boundary
    [InlineData(16384)]  // Exactly at chunk boundary
    [InlineData(16385)]  // Just after chunk boundary
    public void EdgeCase_ChunkBoundaries_NoDataLost(int rowCount)
    {
        // Arrange
        var data = Enumerable.Range(0, rowCount)
            .Select(i => new EdgeCaseRecord { Id = i, Value = i, Score = i / 10.0 })
            .ToList()
            .ToFrozenArrow();

        // Act
        var count = data.AsQueryable().AllowFallback().Count();
        var sum = data.AsQueryable().AllowFallback().Sum(x => x.Value);

        var expectedSum = Enumerable.Range(0, rowCount).Sum();

        // Assert
        Assert.Equal(rowCount, count);
        Assert.Equal(expectedSum, sum);
    }

    [Fact]
    public void EdgeCase_ComplexNestedPredicates_EvaluatedCorrectly()
    {
        // Arrange
        var data = Enumerable.Range(0, 1000)
            .Select(i => new EdgeCaseRecord { Id = i, Value = i, Score = i / 10.0 })
            .ToList()
            .ToFrozenArrow();

        // Act - Chained AND conditions (OR not yet supported)
        var count1 = data.AsQueryable().AllowFallback()
            .Where(x => x.Value > 100)
            .Where(x => x.Value < 900)
            .Count();

        var count2 = data.AsQueryable().AllowFallback()
            .Where(x => x.Value > 100 && x.Value < 900)
            .Count();

        // Both should give same result (logical equivalence)
        var expected = Enumerable.Range(0, 1000)
            .Count(i => i > 100 && i < 900);

        // Assert
        Assert.Equal(expected, count1);
        Assert.Equal(expected, count2);
    }
}

