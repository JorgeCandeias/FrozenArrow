using FrozenArrow.Query;
using System.Collections.Concurrent;

namespace FrozenArrow.Tests.Correctness;

/// <summary>
/// Property-based testing for FrozenArrow operations.
/// Uses randomized inputs to verify correctness properties hold across many scenarios.
/// </summary>
public class PropertyBasedTests
{
    [ArrowRecord]
    public record PropertyTestRecord
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

    /// <summary>
    /// Generates random test data with configurable seed for reproducibility.
    /// </summary>
    private static FrozenArrow<PropertyTestRecord> GenerateRandomData(int rowCount, int seed)
    {
        var random = new Random(seed);
        var records = new List<PropertyTestRecord>(rowCount);

        for (int i = 0; i < rowCount; i++)
        {
            records.Add(new PropertyTestRecord
            {
                Id = i,
                Value = random.Next(-1000, 1000),
                Score = random.NextDouble() * 200.0 - 100.0,
                IsActive = random.NextDouble() > 0.5
            });
        }

        return records.ToFrozenArrow();
    }

    [Theory]
    [InlineData(100, 42)]
    [InlineData(1_000, 123)]
    [InlineData(10_000, 456)]
    [InlineData(100_000, 789)]
    public void Property_FilterResultsAreSubsetOfOriginal(int rowCount, int seed)
    {
        // Arrange
        var data = GenerateRandomData(rowCount, seed);

        // Act
        var filtered = data.AsQueryable().AllowFallback()
            .Where(x => x.Value > 0)
            .ToList();

        // Assert - Property: Filter results must be subset of original
        Assert.True(filtered.Count <= rowCount, 
            "Filtered count must be <= original count");
        Assert.All(filtered, record => Assert.True(record.Value > 0,
            "All filtered records must satisfy predicate"));
    }

    [Theory]
    [InlineData(100, 42)]
    [InlineData(10_000, 123)]
    [InlineData(100_000, 456)]
    public void Property_CountEqualsToListCount(int rowCount, int seed)
    {
        // Arrange
        var data = GenerateRandomData(rowCount, seed);
        var threshold = new Random(seed).Next(-500, 500);

        // Act
        var count = data.AsQueryable().AllowFallback().Where(x => x.Value > threshold).Count();
        var list = data.AsQueryable().AllowFallback().Where(x => x.Value > threshold).ToList();

        // Assert - Property: Count() == ToList().Count
        Assert.Equal(count, list.Count);
    }

    [Theory]
    [InlineData(100, 42)]
    [InlineData(10_000, 123)]
    public void Property_SumIsCommutative(int rowCount, int seed)
    {
        // Arrange
        var data = GenerateRandomData(rowCount, seed);

        // Act - Sum via different paths
        var sum1 = data.AsQueryable().AllowFallback().Sum(x => x.Value);
        var sum2 = data.AsQueryable().AllowFallback().ToList().Sum(x => x.Value);

        // Assert - Property: Sum is associative
        Assert.Equal(sum1, sum2);
    }

    [Theory]
    [InlineData(100, 42, 50)]
    [InlineData(1_000, 123, 100)]
    [InlineData(10_000, 456, 500)]
    public void Property_MultipleFiltersAreConjunctive(int rowCount, int seed, int iterations)
    {
        // Property: WHERE A && WHERE B == WHERE (A && B)
        
        for (int i = 0; i < iterations; i++)
        {
            // Arrange
            var data = GenerateRandomData(rowCount, seed + i);
            var random = new Random(seed + i);
            var threshold1 = random.Next(-500, 500);
            var threshold2 = random.Next(-500, 500); // Changed to int threshold for Value

            // Act
            var separateFilters = data.AsQueryable().AllowFallback()
                .Where(x => x.Value > threshold1)
                .Where(x => x.Value < threshold2)
                .Count();

            var combinedFilter = data.AsQueryable().AllowFallback()
                .Where(x => x.Value > threshold1 && x.Value < threshold2)
                .Count();

            // Assert
            Assert.Equal(separateFilters, combinedFilter);
        }
    }

    [Theory]
    [InlineData(100, 42)]
    [InlineData(10_000, 123)]
    public void Property_AnyIsTrueWhenCountIsNonZero(int rowCount, int seed)
    {
        // Arrange
        var data = GenerateRandomData(rowCount, seed);
        var threshold = new Random(seed).Next(-500, 500);

        // Act
        var any = data.AsQueryable().AllowFallback().Where(x => x.Value > threshold).Any();
        var count = data.AsQueryable().AllowFallback().Where(x => x.Value > threshold).Count();

        // Assert - Property: Any() == true IFF Count() > 0
        Assert.Equal(any, count > 0);
    }

    [Theory]
    [InlineData(100, 42, 20)]
    [InlineData(1_000, 123, 50)]
    public void Property_FirstIsFirstElementOfToList(int rowCount, int seed, int iterations)
    {
        for (int i = 0; i < iterations; i++)
        {
            // Arrange
            var data = GenerateRandomData(rowCount, seed + i);
            var random = new Random(seed + i);
            var threshold = random.Next(-500, 500);

            // Act
            var list = data.AsQueryable().AllowFallback()
                .Where(x => x.Value > threshold)
                .OrderBy(x => x.Id)
                .ToList();

            if (list.Count == 0) continue; // Skip if no matches

            var first = data.AsQueryable().AllowFallback()
                .Where(x => x.Value > threshold)
                .OrderBy(x => x.Id)
                .First();

            // Assert - Property: First() == ToList()[0]
            Assert.Equal(first, list[0]);
        }
    }

    [Theory]
    [InlineData(100, 42, 50)]
    [InlineData(1_000, 123, 100)]
    public void Property_PredicateOrderDoesNotMatterForCount(int rowCount, int seed, int iterations)
    {
        // Property: WHERE A WHERE B == WHERE B WHERE A (for independent predicates)
        
        for (int i = 0; i < iterations; i++)
        {
            // Arrange
            var data = GenerateRandomData(rowCount, seed + i);
            var random = new Random(seed + i);
            var valueThreshold = random.Next(-500, 500);
            var scoreThreshold = random.NextDouble() * 100.0; // Use double for Score comparisons

            // Act
            var order1 = data.AsQueryable().AllowFallback()
                .Where(x => x.Value > valueThreshold)
                .Where(x => x.Score > scoreThreshold)
                .Count();

            var order2 = data.AsQueryable().AllowFallback()
                .Where(x => x.Score > scoreThreshold)
                .Where(x => x.Value > valueThreshold)
                .Count();

            // Assert
            Assert.Equal(order1, order2);
        }
    }

    [Theory]
    [InlineData(100, 42)]
    [InlineData(10_000, 123)]
    public void Property_AverageIsWithinRange(int rowCount, int seed)
    {
        // Arrange
        var data = GenerateRandomData(rowCount, seed);

        // Act
        var filteredList = data.AsQueryable().AllowFallback()
            .Where(x => x.Value > 0)
            .ToList();

        if (filteredList.Count == 0) return; // Skip if no matches

        var average = data.AsQueryable().AllowFallback()
            .Where(x => x.Value > 0)
            .Average(x => x.Score);

        var min = filteredList.Min(x => x.Score);
        var max = filteredList.Max(x => x.Score);

        // Assert - Property: Min <= Average <= Max
        Assert.True(average >= min, $"Average {average} should be >= min {min}");
        Assert.True(average <= max, $"Average {average} should be <= max {max}");
    }

    [Theory]
    [InlineData(100, 42, 100)]
    [InlineData(1_000, 123, 200)]
    public void Property_RandomizedFuzzTesting(int rowCount, int seed, int iterations)
    {
        // Fuzz test: Random queries should never throw or produce invalid results
        
        var random = new Random(seed);
        var exceptions = new ConcurrentBag<Exception>();

        for (int i = 0; i < iterations; i++)
        {
            try
            {
                // Arrange
                var data = GenerateRandomData(rowCount, seed + i);
                var threshold = random.Next(-1000, 1000);
                var scoreThreshold = random.NextDouble() * 200 - 100;

                // Act - Random operation
                var opType = random.Next(0, 5);
                var result = opType switch
                {
                    0 => data.AsQueryable().AllowFallback().Where(x => x.Value > threshold).Count(),
                    1 => data.AsQueryable().AllowFallback().Where(x => x.Score > scoreThreshold).Any() ? 1 : 0,
                    2 => data.AsQueryable().AllowFallback().Where(x => x.Value > threshold).Sum(x => x.Value),
                    3 => data.AsQueryable().AllowFallback().Where(x => x.IsActive).Count(),
                    _ => data.AsQueryable().AllowFallback().Where(x => x.Value > threshold).ToList().Count
                };

                // Assert - Result must be valid (no exceptions thrown)
            }
            catch (Exception ex)
            {
                exceptions.Add(ex);
            }
        }

        // Assert - No exceptions should occur
        Assert.Empty(exceptions);
    }

    [Theory]
    [InlineData(10_000, 42)]
    public void Property_IdempotenceOfReadOperations(int rowCount, int seed)
    {
        // Property: Reading data multiple times produces same results
        
        // Arrange
        var data = GenerateRandomData(rowCount, seed);

        // Act - Execute same query 10 times
        var results = Enumerable.Range(0, 10)
            .Select(_ => data.AsQueryable().AllowFallback()
                .Where(x => x.Value > 0 && x.IsActive)
                .Count())
            .ToList();

        // Assert - Property: Read operations are idempotent
        var first = results[0];
        Assert.All(results, count => Assert.Equal(first, count));
    }

    [Theory]
    [InlineData(100, 42)]
    [InlineData(1_000, 123)]
    public void Property_MonotonicityOfFilters(int rowCount, int seed)
    {
        // Property: More restrictive filter => fewer or equal results
        
        // Arrange
        var data = GenerateRandomData(rowCount, seed);

        // Act
        var count1 = data.AsQueryable().AllowFallback().Where(x => x.Value > -500).Count();
        var count2 = data.AsQueryable().AllowFallback().Where(x => x.Value > 0).Count();
        var count3 = data.AsQueryable().AllowFallback().Where(x => x.Value > 500).Count();

        // Assert - Property: Monotonicity
        Assert.True(count1 >= count2, "Less restrictive filter should have >= results");
        Assert.True(count2 >= count3, "Less restrictive filter should have >= results");
    }
}

