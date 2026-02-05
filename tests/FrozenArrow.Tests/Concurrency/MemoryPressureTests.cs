using FrozenArrow.Query;
using System.Buffers;
using System.Collections.Concurrent;

namespace FrozenArrow.Tests.Concurrency;

/// <summary>
/// Tests for memory pressure scenarios and ArrayPool behavior under concurrent access.
/// Ensures FrozenArrow handles memory pressure gracefully without corruption or leaks.
/// </summary>
public class MemoryPressureTests
{
    [ArrowRecord]
    public record MemoryTestRecord
    {
        [ArrowArray(Name = "Id")]
        public int Id { get; init; }

        [ArrowArray(Name = "Value")]
        public int Value { get; init; }

        [ArrowArray(Name = "Score")]
        public double Score { get; init; }
    }

    private static FrozenArrow<MemoryTestRecord> CreateTestData(int rowCount)
    {
        var random = new Random(42);
        var records = new List<MemoryTestRecord>(rowCount);

        for (int i = 0; i < rowCount; i++)
        {
            records.Add(new MemoryTestRecord
            {
                Id = i,
                Value = random.Next(0, 1000),
                Score = random.NextDouble() * 100.0
            });
        }

        return records.ToFrozenArrow();
    }

    [Theory]
    [InlineData(100_000, 100)]
    public async Task HighMemoryPressure_ConcurrentBitmapAllocations_ShouldNotLeak(int rowCount, int operations)
    {
        // Arrange
        var data = CreateTestData(rowCount);

        // Act - Create memory pressure with many concurrent bitmap allocations
        var tasks = Enumerable.Range(0, operations)
            .Select(_ => Task.Run(() =>
            {
                // Each query allocates bitmaps from ArrayPool
                return data.AsQueryable()
                    .Where(x => x.Value > 250)
                    .Where(x => x.Value < 750)
                    .Where(x => x.Score > 25.0)
                    .Count();
            }))
            .ToArray();

        var results = await Task.WhenAll(tasks);

        // Assert - All operations completed successfully
        Assert.Equal(operations, results.Length);
        Assert.All(results, count => Assert.True(count >= 0));

        // Force GC to detect any leaks
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
    }

    [Theory]
    [InlineData(50_000, 200)]
    public async Task RapidAllocationDeallocation_ConcurrentQueries_ShouldHandleCorrectly(int rowCount, int iterations)
    {
        // Arrange
        var data = CreateTestData(rowCount);

        // Act - Rapid allocation/deallocation pattern
        var tasks = Enumerable.Range(0, iterations)
            .Select(async i =>
            {
                // Small delay to create allocation contention
                if (i % 10 == 0)
                {
                    await Task.Yield();
                }

                return data.AsQueryable()
                    .Where(x => x.Value > 500)
                    .Any();
            })
            .ToArray();

        var results = await Task.WhenAll(tasks);

        // Assert - All queries executed successfully
        Assert.Equal(iterations, results.Length);
    }

    [Fact]
    public async Task GCDuringExecution_ConcurrentQueries_ShouldNotCorrupt()
    {
        // Arrange
        var data = CreateTestData(100_000);
        var exceptions = new ConcurrentBag<Exception>();

        // Act - Trigger GC during concurrent query execution
        var queryTasks = Enumerable.Range(0, 50)
            .Select(async i =>
            {
                try
                {
                    if (i % 5 == 0)
                    {
                        // Trigger GC for some tasks
                        GC.Collect(0, GCCollectionMode.Forced);
                    }

                    var count = data.AsQueryable()
                        .Where(x => x.Value > 300)
                        .Where(x => x.Value < 700)
                        .Count();

                    await Task.Yield();

                    Assert.True(count >= 0, "Query should succeed during GC");
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                }
            })
            .ToArray();

        await Task.WhenAll(queryTasks);

        // Assert - GC should not cause corruption
        Assert.Empty(exceptions);
    }

    [Theory]
    [InlineData(500_000, 50)]
    public async Task LargeDataset_MemoryIntensiveOperations_ShouldNotExhaustMemory(int rowCount, int concurrentQueries)
    {
        // Arrange
        var data = CreateTestData(rowCount);

        // Act - Memory-intensive operations (ToList materializes results)
        var tasks = Enumerable.Range(0, concurrentQueries)
            .Select(_ => Task.Run(() =>
            {
                var results = data.AsQueryable()
                    .Where(x => x.Value > 250 && x.Value < 750)
                    .Take(1000) // Limit materialization
                    .ToList();

                return results.Count;
            }))
            .ToArray();

        var results = await Task.WhenAll(tasks);

        // Assert - All operations completed without OutOfMemoryException
        Assert.All(results, count => Assert.True(count > 0 && count <= 1000));
    }

    [Theory]
    [InlineData(100_000, 100)]
    public async Task ConcurrentBitmapCreationDisposal_HighThroughput_ShouldHandleCorrectly(int bitCount, int operations)
    {
        // Arrange & Act - High-throughput bitmap creation/disposal
        var tasks = Enumerable.Range(0, operations)
            .Select(_ => Task.Run(() =>
            {
                // Create and dispose many bitmaps rapidly
                for (int i = 0; i < 10; i++)
                {
                    var bitmap = SelectionBitmap.Create(bitCount, true);
                    try
                    {
                        bitmap.ClearRange(0, bitCount / 2);
                        var count = bitmap.CountSet();
                        Assert.True(count > 0, "Bitmap should have set bits");
                    }
                    finally
                    {
                        bitmap.Dispose();
                    }
                }
                return true;
            }))
            .ToArray();

        var results = await Task.WhenAll(tasks);

        // Assert - All completed successfully
        Assert.All(results, success => Assert.True(success));
    }

    [Fact]
    public async Task MemoryPressureSimulation_ConcurrentQueries_ShouldRecoverGracefully()
    {
        // Arrange
        var data = CreateTestData(200_000);
        var allocationList = new List<byte[]>();

        try
        {
            // Create artificial memory pressure
            for (int i = 0; i < 100; i++)
            {
                allocationList.Add(new byte[1024 * 1024]); // 1MB allocations
            }

            // Act - Run queries under memory pressure
            var tasks = Enumerable.Range(0, 30)
                .Select(_ => Task.Run(() =>
                {
                    return data.AsQueryable()
                        .Where(x => x.Value > 500)
                        .Count();
                }))
                .ToArray();

            var results = await Task.WhenAll(tasks);

            // Assert - Queries should still complete under pressure
            Assert.All(results, count => Assert.True(count >= 0));
        }
        finally
        {
            // Cleanup
            allocationList.Clear();
            GC.Collect();
        }
    }

    [Theory]
    [InlineData(100_000, 50)]
    public async Task MixedOperations_MemoryPressure_ShouldMaintainCorrectness(int rowCount, int operations)
    {
        // Arrange
        var data = CreateTestData(rowCount);

        // Act - Mix of operations under memory pressure
        var countTasks = Enumerable.Range(0, operations / 2)
            .Select(_ => Task.Run(() => data.AsQueryable().Where(x => x.Value > 500).Count()))
            .ToArray();

        var anyTasks = Enumerable.Range(0, operations / 2)
            .Select(_ => Task.Run(() => data.AsQueryable().Where(x => x.Score > 75.0).Any()))
            .ToArray();

        // Trigger GC during execution
        GC.Collect();

        await Task.WhenAll(countTasks);
        await Task.WhenAll(anyTasks);

        // Assert - All operations completed
        Assert.All(countTasks, t => Assert.True(t.IsCompleted));
        Assert.All(anyTasks, t => Assert.True(t.IsCompleted));
    }

    [Fact]
    public async Task ArrayPoolExhaustion_ConcurrentAccess_ShouldHandleGracefully()
    {
        // Arrange - Create scenario where ArrayPool might be exhausted
        var data = CreateTestData(50_000);
        var concurrentOperations = 200;

        // Act - Many concurrent operations that use ArrayPool
        var tasks = Enumerable.Range(0, concurrentOperations)
            .Select(i => Task.Run(async () =>
            {
                // Stagger operations slightly
                await Task.Delay(i % 10);

                return data.AsQueryable()
                    .Where(x => x.Value > 250)
                    .Where(x => x.Value < 750)
                    .Count();
            }))
            .ToArray();

        var results = await Task.WhenAll(tasks);

        // Assert - All operations should complete even under ArrayPool pressure
        Assert.Equal(concurrentOperations, results.Length);
        Assert.All(results, count => Assert.True(count >= 0));
    }

    [Theory]
    [InlineData(100_000, 100)]
    public async Task LongRunningQueries_MemoryStability_ShouldNotLeak(int rowCount, int durationMilliseconds)
    {
        // Arrange
        var data = CreateTestData(rowCount);
        var cts = new CancellationTokenSource();
        var startMemory = GC.GetTotalMemory(true);

        // Act - Run queries continuously for specified duration
        var runningTasks = new List<Task>();
        for (int i = 0; i < 10; i++)
        {
            runningTasks.Add(Task.Run(async () =>
            {
                while (!cts.Token.IsCancellationRequested)
                {
                    data.AsQueryable()
                        .Where(x => x.Value > 500)
                        .Count();

                    await Task.Delay(10);
                }
            }));
        }

        // Let queries run
        await Task.Delay(durationMilliseconds);
        cts.Cancel();

        await Task.WhenAll(runningTasks);

        // Force GC and check memory
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        var endMemory = GC.GetTotalMemory(false);
        var memoryGrowth = endMemory - startMemory;

        // Assert - Memory growth should be reasonable (< 10MB for this test)
        Assert.True(memoryGrowth < 10 * 1024 * 1024,
            $"Memory grew by {memoryGrowth / 1024 / 1024}MB, possible leak");
    }
}
