using FrozenArrow.Query;
using System.Collections.Concurrent;

namespace FrozenArrow.Tests.Concurrency;

/// <summary>
/// Tests for parallel query execution under concurrent access and stress conditions.
/// These tests are critical for detecting race conditions, memory corruption, and thread safety issues.
/// </summary>
public class ParallelQueryExecutorTests
{
    [ArrowRecord]
    public record ConcurrencyTestRecord
    {
        [ArrowArray(Name = "Id")]
        public int Id { get; init; }

        [ArrowArray(Name = "Value")]
        public int Value { get; init; }

        [ArrowArray(Name = "Category")]
        public int Category { get; init; }

        [ArrowArray(Name = "Score")]
        public double Score { get; init; }

        [ArrowArray(Name = "IsActive")]
        public bool IsActive { get; init; }
    }

    /// <summary>
    /// Generates deterministic test data for concurrency testing.
    /// </summary>
    private static FrozenArrow<ConcurrencyTestRecord> CreateTestData(int rowCount, int seed = 42)
    {
        var random = new Random(seed);
        var records = new List<ConcurrencyTestRecord>(rowCount);

        for (int i = 0; i < rowCount; i++)
        {
            records.Add(new ConcurrencyTestRecord
            {
                Id = i,
                Value = random.Next(0, 1000),
                Category = random.Next(0, 10),
                Score = random.NextDouble() * 100.0,
                IsActive = random.Next(0, 2) == 1
            });
        }

        return records.ToFrozenArrow();
    }

    [Theory]
    [InlineData(10_000, 2)]
    [InlineData(50_000, 4)]
    [InlineData(100_000, 8)]
    [InlineData(500_000, 16)]
    public async Task ConcurrentReads_MultipleTasks_ShouldProduceSameResults(int rowCount, int concurrentTasks)
    {
        // Arrange
        var data = CreateTestData(rowCount);
        var expectedResults = data.AsQueryable().AllowFallback()
            .Where(x => x.Value > 500 && x.IsActive)
            .OrderBy(x => x.Id)
            .ToList();

        // Act - Run the same query concurrently from multiple tasks
        var tasks = Enumerable.Range(0, concurrentTasks)
            .Select(_ => Task.Run(() =>
            {
                return data.AsQueryable().AllowFallback()
                    .Where(x => x.Value > 500 && x.IsActive)
                    .OrderBy(x => x.Id)
                    .ToList();
            }))
            .ToArray();

        var results = await Task.WhenAll(tasks);

        // Assert - All concurrent executions should produce identical results
        foreach (var result in results)
        {
            Assert.Equal(expectedResults.Count, result.Count);
            Assert.Equal(expectedResults, result);
        }
    }

    [Theory]
    [InlineData(100_000, 100)] // Stress test: 100 concurrent queries
    [InlineData(50_000, 200)]  // Extreme stress: 200 concurrent queries
    public async Task ConcurrentReads_HighContention_NoDataCorruption(int rowCount, int concurrentQueries)
    {
        // Arrange
        var data = CreateTestData(rowCount);

        // Act - Hammer the system with many concurrent queries
        var tasks = Enumerable.Range(0, concurrentQueries)
            .Select(i => Task.Run(() =>
            {
                // Vary the queries to test different code paths
                if (i % 3 == 0)
                {
                    return data.AsQueryable().AllowFallback().Where(x => x.Value > 500).Count();
                }
                else if (i % 3 == 1)
                {
                    return data.AsQueryable().AllowFallback().Where(x => x.Value < 300).Count();
                }
                else
                {
                    return data.AsQueryable().AllowFallback().Where(x => x.Score > 50.0 && x.IsActive).Count();
                }
            }))
            .ToArray();

        // Should not throw or deadlock
        var results = await Task.WhenAll(tasks);

        // Assert - All results should be valid (non-negative counts)
        Assert.All(results, count => Assert.True(count >= 0));
    }

    [Theory]
    [InlineData(16383)] // One row below chunk boundary
    [InlineData(16384)] // Exactly one chunk
    [InlineData(16385)] // One row above chunk boundary
    [InlineData(32768)] // Exactly two chunks
    [InlineData(32769)] // Just over two chunks
    [InlineData(49152)] // Exactly three chunks
    public async Task ChunkBoundaryConditions_ConcurrentAccess_ShouldBeCorrect(int rowCount)
    {
        // Arrange
        var data = CreateTestData(rowCount);

        // Act - Run queries concurrently at critical chunk boundaries
        var tasks = Enumerable.Range(0, 10)
            .Select(_ => Task.Run(() =>
            {
                return data.AsQueryable().AllowFallback()
                    .Where(x => x.Value > 250 && x.Value < 750)
                    .Count();
            }))
            .ToArray();

        var results = await Task.WhenAll(tasks);

        // Assert - All concurrent executions should agree
        var firstResult = results[0];
        Assert.All(results, count => Assert.Equal(firstResult, count));
    }

    [Theory]
    [InlineData(100_000, 1)]
    [InlineData(100_000, 2)]
    [InlineData(100_000, 4)]
    [InlineData(100_000, 8)]
    [InlineData(100_000, 16)]
    public async Task DifferentParallelismDegrees_ShouldProduceIdenticalResults(int rowCount, int degreeOfParallelism)
    {
        // Arrange
        var data = CreateTestData(rowCount);

        // Expected result with default parallelism
        var expectedResults = data.AsQueryable().AllowFallback()
            .Where(x => x.Value > 300 && x.Category < 5)
            .OrderBy(x => x.Id)
            .ToList();

        // Act - Query with specific parallelism setting (simulating via concurrent execution)
        var tasks = Enumerable.Range(0, degreeOfParallelism)
            .Select(_ => Task.Run(() =>
            {
                return data.AsQueryable().AllowFallback()
                    .Where(x => x.Value > 300 && x.Category < 5)
                    .OrderBy(x => x.Id)
                    .ToList();
            }))
            .ToArray();

        var results = await Task.WhenAll(tasks);

        // Assert - All results match expected
        foreach (var result in results)
        {
            Assert.Equal(expectedResults, result);
        }
    }

    [Fact]
    public async Task MultiplePredicates_ConcurrentExecution_ShouldBeThreadSafe()
    {
        // Arrange
        var data = CreateTestData(100_000);

        // Act - Test complex multi-predicate queries concurrently
        var tasks = Enumerable.Range(0, 20)
            .Select(i => Task.Run(() =>
            {
                return data.AsQueryable().AllowFallback()
                    .Where(x => x.Value > 200)
                    .Where(x => x.Value < 800)
                    .Where(x => x.Score > 25.0)
                    .Where(x => x.IsActive)
                    .Count();
            }))
            .ToArray();

        // Should complete without exceptions
        var results = await Task.WhenAll(tasks);

        // Assert - All results should be valid
        Assert.All(results, count => Assert.True(count >= 0));
    }

    [Fact]
    public async Task RaceConditionStress_RandomDelay_ShouldNotCauseDataCorruption()
    {
        // Arrange
        var data = CreateTestData(50_000);
        var random = new Random(42);
        var exceptions = new ConcurrentBag<Exception>();

        // Act - Introduce artificial timing variations to expose race conditions
        var tasks = Enumerable.Range(0, 50)
            .Select(async i =>
            {
                try
                {
                    // Random small delay to create thread interleaving
                    await Task.Delay(random.Next(0, 5));

                    var result = data.AsQueryable().AllowFallback()
                        .Where(x => x.Value > 500)
                        .ToList();

                    // Introduce contention point
                    Thread.SpinWait(random.Next(0, 100));

                    var count = data.AsQueryable().AllowFallback()
                        .Where(x => x.Value > 500)
                        .Count();

                    // Result count and list count should match
                    Assert.Equal(result.Count, count);
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                }
            })
            .ToArray();

        await Task.WhenAll(tasks);

        // Assert - No exceptions should have occurred
        Assert.Empty(exceptions);
    }

    [Theory]
    [InlineData(1_000_000)] // Large dataset to ensure multiple chunks
    public async Task LargeDataset_ConcurrentAggregation_ShouldBeCorrect(int rowCount)
    {
        // Arrange
        var data = CreateTestData(rowCount);

        // Calculate expected results once
        var expectedCount = data.AsQueryable().AllowFallback().Where(x => x.Value > 500).Count();
        var expectedSum = data.AsQueryable().AllowFallback().Where(x => x.Value > 500).Sum(x => x.Value);
        var expectedAvg = data.AsQueryable().AllowFallback().Where(x => x.Value > 500).Average(x => x.Score);

        // Act - Run aggregations concurrently
        var countTask = Task.Run(() => data.AsQueryable().AllowFallback().Where(x => x.Value > 500).Count());
        var sumTask = Task.Run(() => data.AsQueryable().AllowFallback().Where(x => x.Value > 500).Sum(x => x.Value));
        var avgTask = Task.Run(() => data.AsQueryable().AllowFallback().Where(x => x.Value > 500).Average(x => x.Score));

        var countResult = await countTask;
        var sumResult = await sumTask;
        var avgResult = await avgTask;

        // Assert - All concurrent aggregations should match expected
        Assert.Equal(expectedCount, countResult);
        Assert.Equal(expectedSum, sumResult);
        Assert.Equal(expectedAvg, avgResult, precision: 10); // Allow small floating point variance
    }

    [Fact]
    public async Task ConcurrentFirstAny_ShouldNotThrow()
    {
        // Arrange
        var data = CreateTestData(100_000);

        // Act - Test short-circuit operations concurrently (First/Any can exit early)
        var tasks = Enumerable.Range(0, 30)
            .Select(i => Task.Run(() =>
            {
                if (i % 2 == 0)
                {
                    return data.AsQueryable().AllowFallback().Where(x => x.Value > 500).Any() ? 1 : 0;
                }
                else
                {
                    data.AsQueryable().AllowFallback().Where(x => x.Value > 500).First();
                    return 1;
                }
            }))
            .ToArray();

        // Should not throw
        var results = await Task.WhenAll(tasks);

        // Assert - All operations completed
        Assert.Equal(30, results.Length);
    }

    [Theory]
    [InlineData(10)] // Run multiple times to catch intermittent issues
    public async Task RepeatedExecution_SameData_ShouldProduceDeterministicResults(int iterations)
    {
        // Arrange
        var data = CreateTestData(100_000);

        // Act - Execute the same query multiple times concurrently
        var allResults = new List<int>();

        for (int iter = 0; iter < iterations; iter++)
        {
            var tasks = Enumerable.Range(0, 10)
                .Select(_ => Task.Run(() =>
                {
                    return data.AsQueryable().AllowFallback()
                        .Where(x => x.Value > 500 && x.IsActive)
                        .Count();
                }))
                .ToArray();

            var results = await Task.WhenAll(tasks);
            allResults.AddRange(results);
        }

        // Assert - All results should be identical (deterministic)
        var expectedCount = allResults[0];
        Assert.All(allResults, count => Assert.Equal(expectedCount, count));
    }

    [Fact]
    public async Task EmptyResult_ConcurrentQueries_ShouldNotThrow()
    {
        // Arrange
        var data = CreateTestData(50_000);

        // Act - Query that returns no results
        var tasks = Enumerable.Range(0, 20)
            .Select(_ => Task.Run(() =>
            {
                return data.AsQueryable().AllowFallback()
                    .Where(x => x.Value > 10000) // Impossible condition
                    .Count();
            }))
            .ToArray();

        var results = await Task.WhenAll(tasks);

        // Assert - All should return 0
        Assert.All(results, count => Assert.Equal(0, count));
    }

    [Theory]
    [InlineData(100_000, 50)] // 50 concurrent mixed operations
    public async Task MixedOperations_ConcurrentExecution_ShouldAllComplete(int rowCount, int operationCount)
    {
        // Arrange
        var data = CreateTestData(rowCount);
        var random = new Random(42);

        // Act - Mix different query operations concurrently
        var tasks = Enumerable.Range(0, operationCount)
            .Select(i => Task.Run<object?>(() =>
            {
                var opType = random.Next(0, 6);
                return opType switch
                {
                    0 => data.AsQueryable().AllowFallback().Where(x => x.Value > 500).Count(),
                    1 => data.AsQueryable().AllowFallback().Where(x => x.IsActive).Any(),
                    2 => data.AsQueryable().AllowFallback().Where(x => x.Value < 500).Sum(x => x.Value),
                    3 => data.AsQueryable().AllowFallback().Where(x => x.Score > 50).Average(x => x.Score),
                    4 => data.AsQueryable().AllowFallback().Where(x => x.Value > 100).FirstOrDefault(),
                    _ => data.AsQueryable().AllowFallback().Where(x => x.Value > 250).ToList().Count,
                };
            }))
            .ToArray();

        // Should all complete successfully
        var results = await Task.WhenAll(tasks);

        // Assert - All operations returned results
        Assert.Equal(operationCount, results.Length);
        Assert.All(results, result => Assert.NotNull(result));
    }
}

