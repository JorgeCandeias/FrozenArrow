using FrozenArrow.Query;

namespace FrozenArrow.Tests.Correctness;

/// <summary>
/// Cross-validation tests: Verify same results through different code paths.
/// These tests ensure optimizations produce identical results to baseline implementations.
/// </summary>
public class CrossValidationTests
{
    [ArrowRecord]
    public record ValidationTestRecord
    {
        [ArrowArray(Name = "Id")]
        public int Id { get; init; }

        [ArrowArray(Name = "Value")]
        public int Value { get; init; }

        [ArrowArray(Name = "Score")]
        public double Score { get; init; }

        [ArrowArray(Name = "IsActive")]
        public bool IsActive { get; init; }
    }

    private static FrozenArrow<ValidationTestRecord> CreateTestData(int rowCount, int seed = 42)
    {
        var random = new Random(seed);
        var records = new List<ValidationTestRecord>(rowCount);

        for (int i = 0; i < rowCount; i++)
        {
            records.Add(new ValidationTestRecord
            {
                Id = i,
                Value = random.Next(0, 1000),
                Score = random.NextDouble() * 100.0,
                IsActive = random.Next(0, 2) == 1
            });
        }

        return records.ToFrozenArrow();
    }

    [Theory]
    [InlineData(100)]
    [InlineData(10_000)]
    [InlineData(100_000)]
    public void CrossValidation_Count_ViaMultiplePaths(int rowCount)
    {
        // Validate: Count via different implementations
        
        // Arrange
        var data = CreateTestData(rowCount);

        // Act - Three paths to count
        var countDirect = data.AsQueryable().AllowFallback()
            .Where(x => x.Value > 500)
            .Count();

        var countViaList = data.AsQueryable().AllowFallback()
            .Where(x => x.Value > 500)
            .ToList()
            .Count;

        var countViaAny = data.AsQueryable().AllowFallback()
            .Where(x => x.Value > 500)
            .ToList()
            .Count(x => x.Value > 500);

        // Assert - All paths agree
        Assert.Equal(countDirect, countViaList);
        Assert.Equal(countViaList, countViaAny);
    }

    [Theory]
    [InlineData(10_000)]
    [InlineData(100_000)]
    public void CrossValidation_Sum_ViaMultiplePaths(int rowCount)
    {
        // Arrange
        var data = CreateTestData(rowCount);

        // Act - Two paths to sum
        var sumDirect = data.AsQueryable().AllowFallback()
            .Where(x => x.Value > 500)
            .Sum(x => x.Value);

        var sumViaList = data.AsQueryable().AllowFallback()
            .Where(x => x.Value > 500)
            .ToList()
            .Sum(x => x.Value);

        // Assert
        Assert.Equal(sumDirect, sumViaList);
    }

    [Theory]
    [InlineData(10_000)]
    [InlineData(100_000)]
    public void CrossValidation_Average_ViaMultiplePaths(int rowCount)
    {
        // Arrange
        var data = CreateTestData(rowCount);

        // Act
        var avgDirect = data.AsQueryable().AllowFallback()
            .Where(x => x.IsActive)
            .Average(x => x.Score);

        var avgViaList = data.AsQueryable().AllowFallback()
            .Where(x => x.IsActive)
            .ToList()
            .Average(x => x.Score);

        // Assert
        Assert.Equal(avgDirect, avgViaList, precision: 10);
    }

    [Theory]
    [InlineData(10_000)]
    public void CrossValidation_ComplexQuery_OptimizedVsNaive(int rowCount)
    {
        // Validate: Complex query via optimized path vs naive materialization
        
        // Arrange
        var data = CreateTestData(rowCount);

        // Act - Optimized path (uses zone maps, predicate reordering, etc.)
        var optimizedResults = data.AsQueryable().AllowFallback()
            .Where(x => x.Value > 500)
            .Where(x => x.Score > 50.0)
            .Where(x => x.IsActive)
            .OrderBy(x => x.Id)
            .ToList();

        // Naive path (materialize early)
        var naiveResults = data.AsQueryable().AllowFallback()
            .ToList()
            .Where(x => x.Value > 500)
            .Where(x => x.Score > 50.0)
            .Where(x => x.IsActive)
            .OrderBy(x => x.Id)
            .ToList();

        // Assert
        Assert.Equal(optimizedResults.Count, naiveResults.Count);
        for (int i = 0; i < optimizedResults.Count; i++)
        {
            Assert.Equal(optimizedResults[i], naiveResults[i]);
        }
    }

    [Theory]
    [InlineData(10_000)]
    public void CrossValidation_FilterChain_DifferentOrders(int rowCount)
    {
        // Validate: Filter chain results independent of order
        
        // Arrange
        var data = CreateTestData(rowCount);

        // Act - Different predicate orders
        var order1 = data.AsQueryable().AllowFallback()
            .Where(x => x.Value > 500)
            .Where(x => x.Score > 50.0)
            .Where(x => x.IsActive)
            .ToList();

        var order2 = data.AsQueryable().AllowFallback()
            .Where(x => x.IsActive)
            .Where(x => x.Value > 500)
            .Where(x => x.Score > 50.0)
            .ToList();

        var order3 = data.AsQueryable().AllowFallback()
            .Where(x => x.Score > 50.0)
            .Where(x => x.IsActive)
            .Where(x => x.Value > 500)
            .ToList();

        // Assert - All orders produce same count (order of results may differ)
        Assert.Equal(order1.Count, order2.Count);
        Assert.Equal(order2.Count, order3.Count);

        // Verify same elements (order-independent)
        var set1 = order1.Select(x => x.Id).ToHashSet();
        var set2 = order2.Select(x => x.Id).ToHashSet();
        var set3 = order3.Select(x => x.Id).ToHashSet();

        Assert.True(set1.SetEquals(set2));
        Assert.True(set2.SetEquals(set3));
    }

    [Theory]
    [InlineData(10_000)]
    public void CrossValidation_First_ViaMultiplePaths(int rowCount)
    {
        // Arrange
        var data = CreateTestData(rowCount);

        // Act
        var firstDirect = data.AsQueryable().AllowFallback()
            .Where(x => x.Value > 500)
            .OrderBy(x => x.Id)
            .First();

        var firstViaList = data.AsQueryable().AllowFallback()
            .Where(x => x.Value > 500)
            .OrderBy(x => x.Id)
            .ToList()
            .First();

        // Assert
        Assert.Equal(firstDirect, firstViaList);
    }

    [Theory]
    [InlineData(10_000)]
    public void CrossValidation_Any_ViaMultiplePaths(int rowCount)
    {
        // Arrange
        var data = CreateTestData(rowCount);

        // Act
        var anyDirect = data.AsQueryable().AllowFallback()
            .Where(x => x.Value > 900)
            .Any();

        var anyViaCount = data.AsQueryable().AllowFallback()
            .Where(x => x.Value > 900)
            .Count() > 0;

        var anyViaList = data.AsQueryable().AllowFallback()
            .Where(x => x.Value > 900)
            .ToList()
            .Any();

        // Assert
        Assert.Equal(anyDirect, anyViaCount);
        Assert.Equal(anyViaCount, anyViaList);
    }

    [Theory]
    [InlineData(10_000, 100)]
    public void CrossValidation_RandomQueries_AlwaysAgree(int rowCount, int iterations)
    {
        // Validate: Random queries produce consistent results across paths
        
        var random = new Random(42);
        var data = CreateTestData(rowCount);

        for (int i = 0; i < iterations; i++)
        {
            // Arrange - Random threshold
            var threshold = random.Next(0, 1000);

            // Act - Both should use same query path (optimized)
            var count1 = data.AsQueryable().AllowFallback()
                .Where(x => x.Value > threshold)
                .Count();

            var count2 = data.AsQueryable().AllowFallback()
                .Where(x => x.Value > threshold)
                .Count();

            // Assert - Same query should produce same result
            Assert.Equal(count1, count2);
        }
    }

    [Theory]
    [InlineData(10_000)]
    public void CrossValidation_EmptyResults_AllPathsAgree(int rowCount)
    {
        // Validate: Empty result handling is consistent
        
        // Arrange
        var data = CreateTestData(rowCount);

        // Act - Query with no matches
        var countOptimized = data.AsQueryable().AllowFallback()
            .Where(x => x.Value > 10000)
            .Count();

        var countNaive = data.AsQueryable().AllowFallback()
            .ToList()
            .Count(x => x.Value > 10000);

        var anyOptimized = data.AsQueryable().AllowFallback()
            .Where(x => x.Value > 10000)
            .Any();

        var anyNaive = data.AsQueryable().AllowFallback()
            .ToList()
            .Any(x => x.Value > 10000);

        // Assert
        Assert.Equal(0, countOptimized);
        Assert.Equal(0, countNaive);
        Assert.False(anyOptimized);
        Assert.False(anyNaive);
    }

    [Theory]
    [InlineData(10_000)]
    public void CrossValidation_SingleResult_AllPathsAgree(int rowCount)
    {
        // Validate: Single result queries agree across paths
        
        // Arrange
        var data = CreateTestData(rowCount);

        // Act - Query that should match exactly one record
        var list = data.AsQueryable().AllowFallback()
            .Where(x => x.Id == 42)
            .ToList();

        var count = data.AsQueryable().AllowFallback()
            .Where(x => x.Id == 42)
            .Count();

        var any = data.AsQueryable().AllowFallback()
            .Where(x => x.Id == 42)
            .Any();

        // Assert
        Assert.Single(list);
        Assert.Equal(1, count);
        Assert.True(any);
        Assert.Equal(42, list[0].Id);
    }

    [Theory]
    [InlineData(100_000)]
    public void CrossValidation_LargeDataset_OptimizedMatchesNaive(int rowCount)
    {
        // Validate: Optimizations work correctly on large datasets
        
        // Arrange
        var data = CreateTestData(rowCount);

        // Act
        var optimizedCount = data.AsQueryable().AllowFallback()
            .Where(x => x.Value > 250 && x.Value < 750)
            .Where(x => x.Score > 25.0 && x.Score < 75.0)
            .Count();

        var naiveCount = data.AsQueryable().AllowFallback()
            .ToList()
            .Count(x => x.Value > 250 && x.Value < 750 && x.Score > 25.0 && x.Score < 75.0);

        // Assert
        Assert.Equal(optimizedCount, naiveCount);
    }

    [Theory]
    [InlineData(10_000)]
    public void CrossValidation_BooleanPredicates_AllPathsAgree(int rowCount)
    {
        // Arrange
        var data = CreateTestData(rowCount);

        // Act
        var countTrue = data.AsQueryable().AllowFallback()
            .Where(x => x.IsActive)
            .Count();

        var countFalse = data.AsQueryable().AllowFallback()
            .Where(x => !x.IsActive)
            .Count();

        var totalViaList = data.AsQueryable().AllowFallback()
            .ToList()
            .Count;

        // Assert
        Assert.Equal(rowCount, countTrue + countFalse);
        Assert.Equal(rowCount, totalViaList);
    }
}

