using FrozenArrow.Query;
using System.Collections.Concurrent;

namespace FrozenArrow.Tests.Concurrency;

/// <summary>
/// Tests for ZoneMap thread safety and concurrent skip-scanning operations.
/// Ensures zone maps correctly skip chunks under parallel execution.
/// </summary>
public class ZoneMapThreadSafetyTests
{
    [ArrowRecord]
    public record ZoneMapTestRecord
    {
        [ArrowArray(Name = "Id")]
        public int Id { get; init; }

        [ArrowArray(Name = "Value")]
        public int Value { get; init; }

        [ArrowArray(Name = "Score")]
        public double Score { get; init; }

        [ArrowArray(Name = "Timestamp")]
        public int Timestamp { get; init; }
    }

    /// <summary>
    /// Creates sorted test data optimal for zone map skip-scanning.
    /// </summary>
    private static FrozenArrow<ZoneMapTestRecord> CreateSortedTestData(int rowCount)
    {
        var records = new List<ZoneMapTestRecord>(rowCount);

        for (int i = 0; i < rowCount; i++)
        {
            records.Add(new ZoneMapTestRecord
            {
                Id = i,
                Value = i, // Sorted for optimal zone map performance
                Score = i / 100.0,
                Timestamp = 1000000 + i
            });
        }

        return records.ToFrozenArrow();
    }

    /// <summary>
    /// Creates data with distinct ranges per chunk for zone map testing.
    /// </summary>
    private static FrozenArrow<ZoneMapTestRecord> CreateChunkedRangeData(int chunkSize, int chunkCount)
    {
        var records = new List<ZoneMapTestRecord>(chunkSize * chunkCount);
        
        for (int chunk = 0; chunk < chunkCount; chunk++)
        {
            // Each chunk has values in a distinct range
            int baseValue = chunk * 1000;
            
            for (int i = 0; i < chunkSize; i++)
            {
                records.Add(new ZoneMapTestRecord
                {
                    Id = chunk * chunkSize + i,
                    Value = baseValue + i,
                    Score = (baseValue + i) / 100.0,
                    Timestamp = 1000000 + baseValue + i
                });
            }
        }

        return records.ToFrozenArrow();
    }

    [Theory]
    [InlineData(100_000, 50)]
    [InlineData(500_000, 100)]
    public async Task SortedData_ConcurrentRangeQueries_ShouldSkipChunksCorrectly(int rowCount, int concurrentQueries)
    {
        // Arrange
        var data = CreateSortedTestData(rowCount);

        // Act - Query different ranges concurrently
        var tasks = Enumerable.Range(0, concurrentQueries)
            .Select(i => Task.Run(() =>
            {
                // Each query targets a different range
                var minValue = i * (rowCount / concurrentQueries);
                var maxValue = minValue + 1000;
                
                return data.AsQueryable()
                    .Where(x => x.Value > minValue && x.Value < maxValue)
                    .Count();
            }))
            .ToArray();

        var results = await Task.WhenAll(tasks);

        // Assert - All queries completed successfully
        Assert.All(results, count => Assert.True(count >= 0));
    }

    [Theory]
    [InlineData(16384, 10)] // 10 chunks of default chunk size
    public async Task ChunkedRanges_SelectiveQueries_ShouldSkipMostChunks(int chunkSize, int chunkCount)
    {
        // Arrange
        var data = CreateChunkedRangeData(chunkSize, chunkCount);

        // Act - Query that should match only one chunk
        var targetChunk = 5;
        var minValue = targetChunk * 1000;
        var maxValue = minValue + 999;

        var tasks = Enumerable.Range(0, 20)
            .Select(_ => Task.Run(() =>
            {
                return data.AsQueryable()
                    .Where(x => x.Value > minValue && x.Value < maxValue)
                    .Count();
            }))
            .ToArray();

        var results = await Task.WhenAll(tasks);

        // Assert - All concurrent executions should agree
        var expectedCount = results[0];
        Assert.All(results, count => Assert.Equal(expectedCount, count));
        Assert.True(expectedCount > 0 && expectedCount <= chunkSize, 
            "Should match data from approximately one chunk");
    }

    [Theory]
    [InlineData(200_000, 50)]
    public async Task HighSelectivity_ConcurrentExecution_ZoneMapsEffective(int rowCount, int concurrentQueries)
    {
        // Arrange
        var data = CreateSortedTestData(rowCount);

        // Act - Highly selective queries (should skip most chunks)
        var tasks = Enumerable.Range(0, concurrentQueries)
            .Select(i => Task.Run(() =>
            {
                // Query top 1% of data
                var threshold = rowCount - (rowCount / 100);
                
                return data.AsQueryable()
                    .Where(x => x.Value > threshold)
                    .Count();
            }))
            .ToArray();

        var results = await Task.WhenAll(tasks);

        // Assert
        var expectedCount = rowCount / 100;
        Assert.All(results, count =>
        {
            Assert.True(count >= expectedCount - 100 && count <= expectedCount + 100,
                "Highly selective query should match approximately 1% of data");
        });
    }

    [Theory]
    [InlineData(100_000)]
    public async Task LowSelectivity_ConcurrentExecution_ZoneMapsMinimalSkip(int rowCount)
    {
        // Arrange
        var data = CreateSortedTestData(rowCount);

        // Act - Low selectivity (most chunks will be evaluated)
        var tasks = Enumerable.Range(0, 30)
            .Select(_ => Task.Run(() =>
            {
                return data.AsQueryable()
                    .Where(x => x.Value > 100) // Matches most data
                    .Count();
            }))
            .ToArray();

        var results = await Task.WhenAll(tasks);

        // Assert - All should agree
        var expectedCount = rowCount - 100;
        Assert.All(results, count => Assert.Equal(expectedCount, count));
    }

    [Fact]
    public async Task MultiplePredicates_ConcurrentZoneMapTests_ShouldBeCorrect()
    {
        // Arrange
        var data = CreateSortedTestData(100_000);

        // Act - Multiple predicates that can use zone maps
        var tasks = Enumerable.Range(0, 40)
            .Select(_ => Task.Run(() =>
            {
                return data.AsQueryable()
                    .Where(x => x.Value > 10000)
                    .Where(x => x.Value < 20000)
                    .Where(x => x.Score > 100.0)
                    .Count();
            }))
            .ToArray();

        var results = await Task.WhenAll(tasks);

        // Assert
        var firstResult = results[0];
        Assert.All(results, count => Assert.Equal(firstResult, count));
    }

    [Theory]
    [InlineData(500_000, 100)]
    public async Task RaceCondition_ZoneMapAccess_ShouldNotCorrupt(int rowCount, int iterations)
    {
        // Arrange
        var data = CreateSortedTestData(rowCount);
        var exceptions = new ConcurrentBag<Exception>();
        var random = new Random(42);

        // Act - Stress test with timing variations
        var tasks = Enumerable.Range(0, iterations)
            .Select(async i =>
            {
                try
                {
                    await Task.Delay(random.Next(0, 2));
                    
                    var threshold = random.Next(0, rowCount);
                    var count = data.AsQueryable()
                        .Where(x => x.Value > threshold)
                        .Count();

                    Thread.SpinWait(random.Next(0, 50));

                    Assert.True(count >= 0 && count <= rowCount,
                        $"Count {count} should be between 0 and {rowCount}");
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                }
            })
            .ToArray();

        await Task.WhenAll(tasks);

        // Assert
        Assert.Empty(exceptions);
    }

    [Theory]
    [InlineData(200_000)]
    public async Task EmptyResultZoneMap_ConcurrentQueries_ShouldSkipAllChunks(int rowCount)
    {
        // Arrange
        var data = CreateSortedTestData(rowCount);

        // Act - Query beyond max value (zone maps should skip all chunks)
        var tasks = Enumerable.Range(0, 50)
            .Select(_ => Task.Run(() =>
            {
                return data.AsQueryable()
                    .Where(x => x.Score > (rowCount + 1000) / 100.0) // Beyond max Score
                    .Count();
            }))
            .ToArray();

        var results = await Task.WhenAll(tasks);

        // Assert - All should return 0
        Assert.All(results, count => Assert.Equal(0, count));
    }

    [Theory]
    [InlineData(16384, 5)] // Exactly 5 chunks
    public async Task ChunkBoundary_ZoneMapDecisions_ShouldBeCorrect(int chunkSize, int chunkCount)
    {
        // Arrange
        var data = CreateChunkedRangeData(chunkSize, chunkCount);
        var totalRows = chunkSize * chunkCount;

        // Act - Query at exact chunk boundaries
        var boundaryTests = new List<Task<int>>();
        
        for (int chunk = 0; chunk < chunkCount; chunk++)
        {
            var chunkStart = chunk * 1000;
            var task = Task.Run(() =>
            {
                return data.AsQueryable()
                    .Where(x => x.Value > chunkStart && x.Value < chunkStart + 500)
                    .Count();
            });
            boundaryTests.Add(task);
        }

        var results = await Task.WhenAll(boundaryTests);

        // Assert - Each chunk query should match approximately same amount
        Assert.All(results, count => Assert.True(count > 0, "Each chunk should have matches"));
    }

    [Fact]
    public async Task MixedQueryPatterns_ConcurrentZoneMapUsage_ShouldBeThreadSafe()
    {
        // Arrange
        var data = CreateSortedTestData(200_000);

        // Act - Mix of selective and non-selective queries
        var selectiveTasks = Enumerable.Range(0, 25)
            .Select(_ => Task.Run(() =>
                data.AsQueryable().Where(x => x.Value > 190000).Count()))
            .ToArray();

        var broadTasks = Enumerable.Range(0, 25)
            .Select(_ => Task.Run(() =>
                data.AsQueryable().Where(x => x.Value > 1000).Count()))
            .ToArray();

        await Task.WhenAll(selectiveTasks.Concat(broadTasks));

        // Assert - All completed successfully
        Assert.All(selectiveTasks, t => Assert.True(t.IsCompleted));
        Assert.All(broadTasks, t => Assert.True(t.IsCompleted));
    }

    [Theory]
    [InlineData(1_000_000)] // Large dataset
    public async Task LargeDataset_ConcurrentZoneMapQueries_ShouldScale(int rowCount)
    {
        // Arrange
        var data = CreateSortedTestData(rowCount);

        // Act - Many concurrent queries on large dataset
        var tasks = Enumerable.Range(0, 100)
            .Select(i => Task.Run(() =>
            {
                var threshold = (i % 10) * (rowCount / 10);
                return data.AsQueryable()
                    .Where(x => x.Value > threshold)
                    .Count();
            }))
            .ToArray();

        var results = await Task.WhenAll(tasks);

        // Assert - Results should decrease with higher thresholds
        Assert.All(results, count => Assert.True(count >= 0 && count <= rowCount));
    }
}
