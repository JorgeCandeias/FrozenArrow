using FrozenArrow.Query;
using System.Collections.Concurrent;

namespace FrozenArrow.Tests.Concurrency;

/// <summary>
/// Tests for QueryPlanCache thread safety and concurrent access patterns.
/// Ensures plan caching works correctly under high concurrency.
/// </summary>
public class QueryPlanCacheTests
{
    [ArrowRecord]
    public record CacheTestRecord
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

    private static FrozenArrow<CacheTestRecord> CreateTestData(int rowCount)
    {
        var random = new Random(42);
        var records = new List<CacheTestRecord>(rowCount);

        for (int i = 0; i < rowCount; i++)
        {
            records.Add(new CacheTestRecord
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
    [InlineData(50_000, 50)]
    [InlineData(100_000, 100)]
    public async Task SameQuery_ConcurrentExecution_ShouldUseCachedPlan(int rowCount, int concurrentQueries)
    {
        // Arrange
        var data = CreateTestData(rowCount);

        // Act - Execute identical query many times concurrently
        var tasks = Enumerable.Range(0, concurrentQueries)
            .Select(_ => Task.Run(() =>
            {
                return data.AsQueryable()
                    .Where(x => x.Value > 500)
                    .Count();
            }))
            .ToArray();

        var results = await Task.WhenAll(tasks);

        // Assert - All should return same result (plan caching working)
        var firstResult = results[0];
        Assert.All(results, count => Assert.Equal(firstResult, count));
    }

    [Theory]
    [InlineData(50_000, 20)]
    public async Task DifferentQueries_ConcurrentExecution_ShouldCacheSeparately(int rowCount, int queryVariations)
    {
        // Arrange
        var data = CreateTestData(rowCount);
        var resultsDict = new ConcurrentDictionary<int, int>();

        // Act - Execute different queries concurrently (different threshold values)
        var tasks = Enumerable.Range(0, queryVariations)
            .Select(i => Task.Run(() =>
            {
                var threshold = 400 + (i * 10); // 400, 410, 420, ...
                var count = data.AsQueryable()
                    .Where(x => x.Value > threshold)
                    .Count();
                
                resultsDict[threshold] = count;
                return count;
            }))
            .ToArray();

        await Task.WhenAll(tasks);

        // Assert - Different thresholds should have been cached separately
        Assert.Equal(queryVariations, resultsDict.Count);
        
        // Verify results are sensible (higher threshold = fewer matches)
        var sortedResults = resultsDict.OrderBy(kvp => kvp.Key).ToList();
        for (int i = 1; i < sortedResults.Count; i++)
        {
            Assert.True(sortedResults[i].Value <= sortedResults[i - 1].Value,
                $"Higher threshold {sortedResults[i].Key} should have <= matches than {sortedResults[i - 1].Key}");
        }
    }

    [Theory]
    [InlineData(100_000, 100)]
    public async Task RepeatedQueries_HighConcurrency_ShouldNotCauseCacheCorruption(int rowCount, int iterations)
    {
        // Arrange
        var data = CreateTestData(rowCount);
        var exceptions = new ConcurrentBag<Exception>();

        // Act - Repeatedly execute queries with high concurrency
        var tasks = Enumerable.Range(0, iterations)
            .Select(async i =>
            {
                try
                {
                    // Mix of different queries to stress cache
                    var query = (i % 3) switch
                    {
                        0 => data.AsQueryable().Where(x => x.Value > 500).Count(),
                        1 => data.AsQueryable().Where(x => x.Score > 50.0).Count(),
                        _ => data.AsQueryable().Where(x => x.IsActive).Count()
                    };

                    await Task.Yield(); // Force async continuation
                    
                    Assert.True(query >= 0, "Query should return non-negative count");
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                }
            })
            .ToArray();

        await Task.WhenAll(tasks);

        // Assert - No cache corruption occurred
        Assert.Empty(exceptions);
    }

    [Fact]
    public async Task ComplexQuery_ConcurrentFirstExecution_ShouldCacheCorrectly()
    {
        // Arrange
        var data = CreateTestData(100_000);

        // Act - First execution of complex query from multiple threads simultaneously
        var tasks = Enumerable.Range(0, 20)
            .Select(_ => Task.Run(() =>
            {
                return data.AsQueryable()
                    .Where(x => x.Value > 200)
                    .Where(x => x.Value < 800)
                    .Where(x => x.Score > 25.0)
                    .Count();
            }))
            .ToArray();

        var results = await Task.WhenAll(tasks);

        // Assert - All concurrent first executions should agree
        var firstResult = results[0];
        Assert.All(results, count => Assert.Equal(firstResult, count));
    }

    [Theory]
    [InlineData(50_000, 50)]
    public async Task MixedOperations_ConcurrentCacheAccess_ShouldBeThreadSafe(int rowCount, int operations)
    {
        // Arrange
        var data = CreateTestData(rowCount);

        // Act - Mix of Count, Any, First operations (different query types)
        var tasks = Enumerable.Range(0, operations)
            .Select(i => Task.Run<object>(() =>
            {
                return (i % 3) switch
                {
                    0 => data.AsQueryable().Where(x => x.Value > 500).Count(),
                    1 => data.AsQueryable().Where(x => x.Score > 75.0).Any(),
                    _ => data.AsQueryable().Where(x => x.Value > 100).FirstOrDefault() ?? new CacheTestRecord()
                };
            }))
            .ToArray();

        var results = await Task.WhenAll(tasks);

        // Assert - All operations completed successfully
        Assert.Equal(operations, results.Length);
        Assert.All(results, result => Assert.NotNull(result));
    }

    [Fact]
    public async Task CacheWarmup_ParallelQueries_ShouldNotDeadlock()
    {
        // Arrange
        var data = CreateTestData(100_000);
        var queryCount = 10;

        // Act - Execute multiple different queries in parallel (cache warmup scenario)
        var warmupTasks = Enumerable.Range(0, queryCount)
            .Select(i => Task.Run(() =>
            {
                return data.AsQueryable()
                    .Where(x => x.Value > (i * 50))
                    .Count();
            }))
            .ToArray();

        // Should complete within reasonable time (no deadlock)
        var timeout = Task.Delay(TimeSpan.FromSeconds(10));
        var completedTask = await Task.WhenAny(Task.WhenAll(warmupTasks), timeout);

        // Check if any tasks faulted
        var faultedTasks = warmupTasks.Where(t => t.IsFaulted).ToList();
        if (faultedTasks.Any())
        {
            var exceptions = faultedTasks.Select(t => t.Exception?.InnerException?.Message ?? "Unknown").ToList();
            Assert.Fail($"Tasks faulted: {string.Join(", ", exceptions)}");
        }

        // Assert
        Assert.True(completedTask == Task.WhenAll(warmupTasks), "Cache warmup should not deadlock");
    }

    [Theory]
    [InlineData(100_000, 100)]
    public async Task DeterministicResults_WithCaching_ShouldBeConsistent(int rowCount, int repetitions)
    {
        // Arrange
        var data = CreateTestData(rowCount);

        // First execution to populate cache
        var expectedCount = data.AsQueryable()
            .Where(x => x.Value > 500 && x.IsActive)
            .Count();

        // Act - Repeat query many times (should use cached plan)
        var tasks = Enumerable.Range(0, repetitions)
            .Select(_ => Task.Run(() =>
            {
                return data.AsQueryable()
                    .Where(x => x.Value > 500 && x.IsActive)
                    .Count();
            }))
            .ToArray();

        var results = await Task.WhenAll(tasks);

        // Assert - Cached plan should produce same results
        Assert.All(results, count => Assert.Equal(expectedCount, count));
    }

    [Fact]
    public async Task LongRunningQueries_ConcurrentCacheAccess_ShouldNotBlock()
    {
        // Arrange
        var largeData = CreateTestData(500_000);
        var smallData = CreateTestData(10_000);

        // Act - Mix of fast and slow queries
        var longQuery = Task.Run(() =>
        {
            return largeData.AsQueryable()
                .Where(x => x.Value > 250)
                .Count();
        });

        var shortQueries = Enumerable.Range(0, 10)
            .Select(_ => Task.Run(() =>
            {
                return smallData.AsQueryable()
                    .Where(x => x.Value > 500)
                    .Count();
            }))
            .ToArray();

        // All should complete
        await Task.WhenAll(shortQueries);
        await longQuery;

        // Assert - Short queries should not be blocked by long query
        Assert.True(longQuery.IsCompleted);
        Assert.All(shortQueries, t => Assert.True(t.IsCompleted));
    }
}
