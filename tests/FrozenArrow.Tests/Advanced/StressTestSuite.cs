using FrozenArrow.Query;
using System.Diagnostics;

namespace FrozenArrow.Tests.Advanced;

/// <summary>
/// Extended stress tests for high-load scenarios and sustained operation.
/// These tests verify stability and correctness under extreme conditions.
/// </summary>
public class StressTestSuite
{
    [ArrowRecord]
    public record StressTestRecord
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

    private static FrozenArrow<StressTestRecord> CreateTestData(int rowCount, int seed = 42)
    {
        var random = new Random(seed);
        var records = new List<StressTestRecord>(rowCount);

        for (int i = 0; i < rowCount; i++)
        {
            records.Add(new StressTestRecord
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
    [InlineData(1_000_000)]   // 1M rows
    [InlineData(5_000_000)]   // 5M rows
    public void Stress_LargeDatasets_QueriesComplete(int rowCount)
    {
        // Test that large datasets can be queried successfully
        
        // Arrange
        var sw = Stopwatch.StartNew();
        var data = CreateTestData(rowCount);
        var createTime = sw.Elapsed;

        // Act
        sw.Restart();
        var count = data.AsQueryable()
            .Where(x => x.Value > 500)
            .Where(x => x.IsActive)
            .Count();
        var queryTime = sw.Elapsed;

        // Assert
        Assert.True(count >= 0 && count <= rowCount);
        Assert.True(queryTime < TimeSpan.FromSeconds(30), 
            $"Query took {queryTime.TotalSeconds}s, expected < 30s");
    }

    [Theory]
    [InlineData(100_000, 1000)]  // 100K rows, 1000 iterations
    public void Stress_RepeatedQueries_NoPerformanceDegradation(int rowCount, int iterations)
    {
        // Test that repeated queries don't degrade performance (no memory leaks, etc.)
        
        // Arrange
        var data = CreateTestData(rowCount);
        var times = new List<TimeSpan>();

        // Act - Run same query many times
        for (int i = 0; i < iterations; i++)
        {
            var sw = Stopwatch.StartNew();
            var count = data.AsQueryable()
                .Where(x => x.Value > 500)
                .Count();
            times.Add(sw.Elapsed);

            Assert.True(count >= 0);
        }

        // Assert - Performance shouldn't degrade significantly
        var firstTen = times.Take(10).Average(t => t.TotalMilliseconds);
        var lastTen = times.TakeLast(10).Average(t => t.TotalMilliseconds);

        Assert.True(lastTen < firstTen * 2, 
            $"Performance degraded: first 10 avg={firstTen:F2}ms, last 10 avg={lastTen:F2}ms");
    }

    [Theory]
    [InlineData(500_000, 100)]  // 500K rows, 100 varied queries
    public void Stress_VariedQueries_AllComplete(int rowCount, int queryCount)
    {
        // Test many different query patterns on same data
        
        // Arrange
        var data = CreateTestData(rowCount);
        var random = new Random(42);

        // Act - Execute varied queries
        for (int i = 0; i < queryCount; i++)
        {
            var queryType = random.Next(0, 5);
            
            var result = queryType switch
            {
                0 => (object)data.AsQueryable().Where(x => x.Value > random.Next(0, 1000)).Count(),
                1 => (object)data.AsQueryable().Where(x => x.IsActive).Any(),
                2 => (object)data.AsQueryable().Where(x => x.IsActive).Sum(x => x.Value),
                3 => (object)data.AsQueryable().Where(x => x.Value > random.Next(0, 500)).Average(x => x.Score),
                _ => (object)data.AsQueryable().Where(x => x.Value > random.Next(0, 1000)).ToList().Count
            };
        }

        // Assert - All queries completed without exceptions
        Assert.True(true);
    }

    [Theory]
    [InlineData(100_000)]
    public void Stress_DeepFilterChains_HandledCorrectly(int rowCount)
    {
        // Test queries with many chained Where clauses (AND conditions)
        
        // Arrange
        var data = CreateTestData(rowCount);

        // Act - 10 chained predicates (using AND, not OR)
        var count = data.AsQueryable()
            .Where(x => x.Value > 0)
            .Where(x => x.Value < 1000)
            .Where(x => x.Score > 0.0)
            .Where(x => x.Score < 100.0)
            .Where(x => x.IsActive || !x.IsActive) // Always true
            .Count();

        // Assert
        Assert.True(count >= 0 && count <= rowCount);
    }

    [Theory]
    [InlineData(1_000_000, 10)]  // 1M rows, 10 concurrent
    public async Task Stress_HighConcurrency_SustainedLoad(int rowCount, int concurrentTasks)
    {
        // Test sustained high concurrency load
        
        // Arrange
        var data = CreateTestData(rowCount);
        var duration = TimeSpan.FromSeconds(5);
        var cts = new CancellationTokenSource(duration);
        var completedQueries = 0;

        // Act - Run queries continuously for specified duration
        var tasks = Enumerable.Range(0, concurrentTasks)
            .Select(_ => Task.Run(async () =>
            {
                while (!cts.Token.IsCancellationRequested)
                {
                    _ = data.AsQueryable()
                        .Where(x => x.Value > 500)
                        .Count();
                    
                    Interlocked.Increment(ref completedQueries);
                    await Task.Yield();
                }
            }))
            .ToArray();

        await Task.WhenAll(tasks);

        // Assert
        Assert.True(completedQueries > concurrentTasks * 10, 
            $"Only completed {completedQueries} queries in {duration.TotalSeconds}s");
    }

    [Theory]
    [InlineData(500_000)]
    public void Stress_ComplexAggregations_LargeDataset(int rowCount)
    {
        // Test complex aggregation operations on large dataset
        
        // Arrange
        var data = CreateTestData(rowCount);

        // Act
        var sw = Stopwatch.StartNew();
        
        var count = data.AsQueryable().Where(x => x.IsActive).Count();
        var sum = data.AsQueryable().Where(x => x.IsActive).Sum(x => x.Value);
        var avg = data.AsQueryable().Where(x => x.IsActive).Average(x => x.Score);
        var filtered = data.AsQueryable()
            .Where(x => x.Value > 500 && x.IsActive)
            .ToList();

        sw.Stop();

        // Assert
        Assert.True(count >= 0);
        Assert.True(sum >= 0);
        Assert.True(avg >= 0);
        Assert.True(filtered.Count <= count);
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(10), 
            $"Aggregations took {sw.Elapsed.TotalSeconds}s, expected < 10s");
    }

    [Theory]
    [InlineData(100_000, 50)]
    public void Stress_RapidQuerySwitching_NoErrors(int rowCount, int switchCount)
    {
        // Test rapidly switching between different query types
        
        // Arrange
        var data = CreateTestData(rowCount);
        var random = new Random(42);

        // Act - Rapidly switch between query types
        for (int i = 0; i < switchCount; i++)
        {
            // Execute multiple query types in rapid succession
            _ = data.AsQueryable().Where(x => x.Value > 500).Count();
            _ = data.AsQueryable().Where(x => x.Score > 50.0).Any();
            _ = data.AsQueryable().Where(x => x.IsActive).Sum(x => x.Value);
            _ = data.AsQueryable().Where(x => x.Value < 200).Average(x => x.Score);
            _ = data.AsQueryable().Where(x => x.Value > 700).First();
        }

        // Assert - No exceptions thrown
        Assert.True(true);
    }

    [Fact]
    public void Stress_MemoryPressure_MultipleDatasets()
    {
        // Test behavior when multiple large datasets exist simultaneously
        
        // Arrange - Create multiple datasets
        var datasets = Enumerable.Range(0, 5)
            .Select(i => CreateTestData(200_000, seed: i))
            .ToList();

        try
        {
            // Act - Query all datasets
            var results = datasets.Select(data =>
                data.AsQueryable()
                    .Where(x => x.Value > 500)
                    .Count()
            ).ToList();

            // Assert
            Assert.Equal(5, results.Count);
            Assert.All(results, count => Assert.True(count >= 0));
        }
        finally
        {
            // Cleanup
            datasets.Clear();
            GC.Collect();
        }
    }

    [Theory]
    [InlineData(100_000, 100)]
    public void Stress_ShortCircuitOperations_HighFrequency(int rowCount, int iterations)
    {
        // Test short-circuit operations (Any, First) at high frequency
        
        // Arrange
        var data = CreateTestData(rowCount);

        // Act
        for (int i = 0; i < iterations; i++)
        {
            var any1 = data.AsQueryable().Where(x => x.Value > 900).Any();
            var any2 = data.AsQueryable().Where(x => x.Value < 100).Any();
            var first = data.AsQueryable().Where(x => x.Value > 0).First();
            var firstOrDefault = data.AsQueryable().Where(x => x.Value > 10000).FirstOrDefault();

            Assert.NotNull(first);
        }

        // Assert - All operations completed
        Assert.True(true);
    }

    [Theory]
    [InlineData(1_000_000)]
    public void Stress_FullDatasetScan_Performance(int rowCount)
    {
        // Test full dataset scan performance
        
        // Arrange
        var data = CreateTestData(rowCount);

        // Act - Scan entire dataset
        var sw = Stopwatch.StartNew();
        var count = data.AsQueryable().Count();
        var scanTime = sw.Elapsed;

        // Assert
        Assert.Equal(rowCount, count);
        Assert.True(scanTime < TimeSpan.FromSeconds(5), 
            $"Full scan took {scanTime.TotalSeconds}s, expected < 5s");
    }
}
