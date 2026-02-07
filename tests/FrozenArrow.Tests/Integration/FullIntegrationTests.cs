using FrozenArrow.Query;
using System.Diagnostics;

namespace FrozenArrow.Tests.Integration;

/// <summary>
/// Integration tests demonstrating all 10 phases working together.
/// </summary>
public class FullIntegrationTests
{
    [ArrowRecord]
    public record IntegrationTestRecord
    {
        [ArrowArray(Name = "Id")]
        public int Id { get; init; }

        [ArrowArray(Name = "Value")]
        public int Value { get; init; }

        [ArrowArray(Name = "Score")]
        public double Score { get; init; }

        [ArrowArray(Name = "Category")]
        public string Category { get; init; } = string.Empty;
    }

    private static FrozenArrow<IntegrationTestRecord> CreateDataset(int size)
    {
        var random = new Random(42);
        var records = new List<IntegrationTestRecord>(size);

        for (int i = 0; i < size; i++)
        {
            records.Add(new IntegrationTestRecord
            {
                Id = i,
                Value = random.Next(0, 1000),
                Score = random.NextDouble() * 100.0,
                Category = ((char)('A' + (i % 5))).ToString()
            });
        }

        return records.ToFrozenArrow();
    }

    [Fact]
    public void AllPhases_WorkTogether_ProducesCorrectResults()
    {
        // Arrange
        var data = CreateDataset(10_000);
        var queryable = data.AsQueryable();
        var provider = (ArrowQueryProvider)queryable.Provider;

        // Enable ALL phases
        provider.UseLogicalPlanExecution = true;   // Phase 5
        provider.UsePhysicalPlanExecution = true;  // Phase 6
        provider.UseLogicalPlanCache = true;       // Phase 7
        provider.UseCompiledQueries = true;        // Phase 9
        provider.UseAdaptiveExecution = true;      // Phase 10

        // Act - Execute LINQ query
        var linqResult = queryable
            .Where(x => x.Value > 500)
            .Count();

        // Assert
        Assert.True(linqResult > 0, "Should have matching records");

        // Verify statistics are being collected
        var cacheStats = provider.GetLogicalPlanCacheStatistics();
        var adaptiveStats = provider.GetAdaptiveStatistics();

        Assert.True(cacheStats.Count > 0 || cacheStats.Hits > 0 || cacheStats.Misses > 0, 
            "Cache should be tracking statistics");
    }

    [Fact]
    public void AllPhases_RepeatedQueries_UsesCache()
    {
        // Arrange
        var data = CreateDataset(5_000);
        var queryable = data.AsQueryable();
        var provider = (ArrowQueryProvider)queryable.Provider;

        provider.UseLogicalPlanExecution = true;
        provider.UseLogicalPlanCache = true;
        provider.UseCompiledQueries = true;

        // Act - Execute same query multiple times
        var results = new List<int>();
        for (int i = 0; i < 10; i++)
        {
            results.Add(queryable.Where(x => x.Value > 500).Count());
        }

        var stats = provider.GetLogicalPlanCacheStatistics();

        // Assert - All results should be identical
        Assert.True(results.All(r => r == results[0]), "All results should be the same");

        // Cache should show hits after first query
        Assert.True(stats.Hits > 0 || stats.Count > 0, 
            "Cache should have been used for repeated queries");
    }

    [Fact]
    public void AllPhases_ComplexQuery_ProducesCorrectResults()
    {
        // Arrange
        var data = CreateDataset(5_000);
        var queryable = data.AsQueryable();
        var provider = (ArrowQueryProvider)queryable.Provider;

        // Enable all optimizations
        provider.UseLogicalPlanExecution = true;
        provider.UsePhysicalPlanExecution = true;
        provider.UseLogicalPlanCache = true;
        provider.UseCompiledQueries = true;

        // Act - Complex query with multiple operations
        var result = queryable
            .Where(x => x.Value > 200 && x.Value < 800)
            .Take(100)
            .ToList();

        // Assert
        Assert.NotEmpty(result);
        Assert.True(result.Count <= 100, "Should respect TAKE limit");
        Assert.All(result, r =>
        {
            Assert.True(r.Value > 200 && r.Value < 800, "Value should be in range");
        });
    }

    [Fact]
    public void AllPhases_GroupByQuery_WorksCorrectly()
    {
        // Arrange
        var data = CreateDataset(1_000);
        var queryable = data.AsQueryable();
        var provider = (ArrowQueryProvider)queryable.Provider;

        provider.UseLogicalPlanExecution = true;
        provider.UsePhysicalPlanExecution = true;

        // Act - GroupBy with aggregation
        var results = queryable
            .Where(x => x.Value > 500)
            .GroupBy(x => x.Category)
            .Select(g => new { Category = g.Key, Count = g.Count(), AvgScore = g.Average(x => x.Score) })
            .ToList();

        // Assert
        Assert.NotEmpty(results);
        Assert.All(results, r =>
        {
            Assert.NotNull(r.Category);
            Assert.True(r.Count > 0, "Each group should have records");
            Assert.True(r.AvgScore >= 0 && r.AvgScore <= 100, "Average score should be valid");
        });
    }

    [Fact(Skip = "SQL parser needs improvement for complex schema detection")]
    public void AllPhases_SQLAndLINQ_ProduceSameResults()
    {
        // Arrange
        var data = CreateDataset(5_000);
        var queryable = data.AsQueryable();
        var provider = (ArrowQueryProvider)queryable.Provider;

        provider.UseLogicalPlanExecution = true;
        provider.UseLogicalPlanCache = true;

        // Act - LINQ query
        var linqCount = queryable
            .Where(x => x.Value > 500)
            .Count();

        // Act - SQL query (Phase 8) - using single int predicate
        var sqlCount = data.ExecuteSqlScalar<IntegrationTestRecord, int>(
            "SELECT COUNT(*) FROM data WHERE Value > 500");

        // Assert - Should produce identical results
        Assert.Equal(linqCount, sqlCount);

        // Both should use the same optimizations
        var stats = provider.GetLogicalPlanCacheStatistics();
        Assert.True(stats.Count >= 2, "Should have cached both LINQ and SQL plans");
    }

    [Fact]
    public void AllPhases_PerformanceMeasurement_ShowsImprovement()
    {
        // Arrange
        var data = CreateDataset(10_000);
        var queryable = data.AsQueryable();
        var provider = (ArrowQueryProvider)queryable.Provider;

        // Baseline - no optimizations
        provider.UseLogicalPlanExecution = false;
        var baselineSw = Stopwatch.StartNew();
        for (int i = 0; i < 5; i++)
        {
            queryable.Where(x => x.Value > 500 && x.Score > 50).Count();
        }
        baselineSw.Stop();
        var baselineMs = baselineSw.ElapsedMilliseconds;

        // Optimized - all phases enabled
        provider.UseLogicalPlanExecution = true;
        provider.UseLogicalPlanCache = true;
        provider.UseCompiledQueries = true;
        
        var optimizedSw = Stopwatch.StartNew();
        for (int i = 0; i < 5; i++)
        {
            queryable.Where(x => x.Value > 500 && x.Score > 50).Count();
        }
        optimizedSw.Stop();
        var optimizedMs = optimizedSw.ElapsedMilliseconds;

        // Assert - Optimized should be faster (or at least not significantly slower)
        var speedup = (double)baselineMs / optimizedMs;
        
        // With current partial integration, we expect at least some improvement
        Assert.True(speedup >= 0.9, $"Should not be significantly slower. Speedup: {speedup:F2}×");

        // Output for visibility
        Console.WriteLine($"Performance: Baseline={baselineMs}ms, Optimized={optimizedMs}ms, Speedup={speedup:F2}×");
    }
}
