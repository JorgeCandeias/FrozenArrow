using FrozenArrow.Query;
using System.Collections.Concurrent;

namespace FrozenArrow.Tests.Concurrency;

/// <summary>
/// Tests for PredicateReorderer determinism and thread safety.
/// Ensures predicate reordering produces consistent results under concurrent execution.
/// </summary>
public class PredicateReorderingTests
{
    [ArrowRecord]
    public record ReorderTestRecord
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

    private static FrozenArrow<ReorderTestRecord> CreateTestData(int rowCount)
    {
        var random = new Random(42);
        var records = new List<ReorderTestRecord>(rowCount);

        for (int i = 0; i < rowCount; i++)
        {
            records.Add(new ReorderTestRecord
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
    [InlineData(100_000, 50)]
    [InlineData(500_000, 100)]
    public async Task MultiPredicateQuery_ConcurrentExecution_ShouldBeDeterministic(int rowCount, int concurrentQueries)
    {
        // Arrange
        var data = CreateTestData(rowCount);

        // Act
        var tasks = Enumerable.Range(0, concurrentQueries)
            .Select(_ => Task.Run(() =>
            {
                return data.AsQueryable()
                    .Where(x => x.Value > 500)
                    .Where(x => x.Score > 50.0)
                    .Where(x => x.IsActive)
                    .Count();
            }))
            .ToArray();

        var results = await Task.WhenAll(tasks);

        // Assert
        var expectedCount = results[0];
        Assert.All(results, count => Assert.Equal(expectedCount, count));
    }

    [Theory]
    [InlineData(100_000, 100)]
    public async Task PredicateReordering_HighConcurrency_ShouldNotCauseRaceConditions(int rowCount, int iterations)
    {
        // Arrange
        var data = CreateTestData(rowCount);
        var exceptions = new ConcurrentBag<Exception>();

        // Act
        var tasks = Enumerable.Range(0, iterations)
            .Select(async i =>
            {
                try
                {
                    var count = (i % 3) switch
                    {
                        0 => data.AsQueryable()
                            .Where(x => x.Value > 400)
                            .Where(x => x.Score > 40.0)
                            .Count(),
                        
                        1 => data.AsQueryable()
                            .Where(x => x.Score > 50.0)
                            .Where(x => x.IsActive)
                            .Count(),
                        
                        _ => data.AsQueryable()
                            .Where(x => x.Value > 300)
                            .Where(x => x.Score > 30.0)
                            .Where(x => x.IsActive)
                            .Count()
                    };

                    await Task.Yield();
                    Assert.True(count >= 0);
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
    [InlineData(100_000)]
    public async Task DifferentPredicateOrder_SameLogic_ShouldProduceSameResults(int rowCount)
    {
        // Arrange
        var data = CreateTestData(rowCount);

        // Act
        var order1Tasks = Enumerable.Range(0, 25)
            .Select(_ => Task.Run(() =>
            {
                return data.AsQueryable()
                    .Where(x => x.Value > 500)
                    .Where(x => x.Score > 50.0)
                    .Count();
            }))
            .ToArray();

        var order2Tasks = Enumerable.Range(0, 25)
            .Select(_ => Task.Run(() =>
            {
                return data.AsQueryable()
                    .Where(x => x.Score > 50.0)  // Different order
                    .Where(x => x.Value > 500)
                    .Count();
            }))
            .ToArray();

        var results1 = await Task.WhenAll(order1Tasks);
        var results2 = await Task.WhenAll(order2Tasks);

        // Assert
        var expected = results1[0];
        Assert.All(results1, count => Assert.Equal(expected, count));
        Assert.All(results2, count => Assert.Equal(expected, count));
    }

    [Theory]
    [InlineData(50_000, 50)]
    public async Task SelectivityEstimation_ConcurrentQueries_ShouldBeConsistent(int rowCount, int concurrentQueries)
    {
        // Arrange
        var data = CreateTestData(rowCount);

        // Act
        var tasks = Enumerable.Range(0, concurrentQueries)
            .Select(_ => Task.Run(() =>
            {
                return data.AsQueryable()
                    .Where(x => x.Value > 800)
                    .Where(x => x.Score > 75.0)
                    .Count();
            }))
            .ToArray();

        var results = await Task.WhenAll(tasks);

        // Assert
        var expectedCount = results[0];
        Assert.All(results, count => Assert.Equal(expectedCount, count));
    }

    [Fact]
    public async Task ComplexPredicateChain_ConcurrentReordering_ShouldComplete()
    {
        // Arrange
        var data = CreateTestData(100_000);

        // Act
        var tasks = Enumerable.Range(0, 30)
            .Select(_ => Task.Run(() =>
            {
                return data.AsQueryable()
                    .Where(x => x.Value > 100)
                    .Where(x => x.Value < 900)
                    .Where(x => x.Score > 10.0)
                    .Where(x => x.Score < 90.0)
                    .Where(x => x.IsActive)
                    .Count();
            }))
            .ToArray();

        var results = await Task.WhenAll(tasks);

        // Assert - All completed successfully
        Assert.All(results, count => Assert.True(count >= 0));
    }

    [Theory]
    [InlineData(100_000, 50)]
    public async Task MixedQueryComplexity_ConcurrentReordering_ShouldBeThreadSafe(int rowCount, int operations)
    {
        // Arrange
        var data = CreateTestData(rowCount);

        // Act
        var tasks = Enumerable.Range(0, operations)
            .Select(i => Task.Run(() =>
            {
                if (i % 2 == 0)
                {
                    return data.AsQueryable()
                        .Where(x => x.Value > 500)
                        .Count();
                }
                else
                {
                    return data.AsQueryable()
                        .Where(x => x.Value > 300)
                        .Where(x => x.Score > 40.0)
                        .Where(x => x.IsActive)
                        .Count();
                }
            }))
            .ToArray();

        var results = await Task.WhenAll(tasks);

        // Assert
        Assert.Equal(operations, results.Length);
        Assert.All(results, count => Assert.True(count >= 0));
    }

    [Theory]
    [InlineData(200_000, 100)]
    public async Task RepeatedReordering_SamePredicates_ShouldBeDeterministic(int rowCount, int repetitions)
    {
        // Arrange
        var data = CreateTestData(rowCount);

        // Act
        var tasks = Enumerable.Range(0, repetitions)
            .Select(_ => Task.Run(() =>
            {
                return data.AsQueryable()
                    .Where(x => x.Value > 400)
                    .Where(x => x.Score > 40.0)
                    .Count();
            }))
            .ToArray();

        var results = await Task.WhenAll(tasks);

        // Assert
        var expectedCount = results[0];
        Assert.All(results, count => Assert.Equal(expectedCount, count));
    }

    [Fact]
    public async Task RaceCondition_PredicateAnalysis_ShouldNotCorruptState()
    {
        // Arrange
        var data = CreateTestData(50_000);
        var random = new Random(42);
        var exceptions = new ConcurrentBag<Exception>();

        // Act
        var tasks = Enumerable.Range(0, 100)
            .Select(async i =>
            {
                try
                {
                    await Task.Delay(random.Next(0, 3));
                    
                    var count = data.AsQueryable()
                        .Where(x => x.Value > 500)
                        .Where(x => x.Score > 50.0)
                        .Count();

                    Thread.SpinWait(random.Next(0, 100));

                    Assert.True(count >= 0);
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
    [InlineData(100_000, 20)]
    public async Task ZoneMapIntegration_PredicateReordering_ShouldOptimizeSkipping(int rowCount, int concurrentQueries)
    {
        // Arrange - Create sorted data for zone maps
        var records = Enumerable.Range(0, rowCount)
            .Select(i => new ReorderTestRecord
            {
                Id = i,
                Value = i,
                Score = i / 100.0,
                IsActive = i % 2 == 0
            })
            .ToList();
        var data = records.ToFrozenArrow();

        // Act - Selective query that should skip most chunks via zone maps
        var tasks = Enumerable.Range(0, concurrentQueries)
            .Select(_ => Task.Run(() =>
            {
                return data.AsQueryable()
                    .Where(x => x.Value > rowCount - 1000)
                    .Where(x => x.Score > (rowCount - 1000) / 100.0)
                    .Count();
            }))
            .ToArray();

        var results = await Task.WhenAll(tasks);

        // Assert - All should agree
        var expectedCount = results[0];
        Assert.True(expectedCount > 0 && expectedCount <= 1000, 
            "Should match approximately last 1000 rows");
        Assert.All(results, count => Assert.Equal(expectedCount, count));
    }
}
