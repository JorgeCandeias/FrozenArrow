using FrozenArrow.Query;

namespace FrozenArrow.Tests.LogicalPlan;

/// <summary>
/// Tests for Phase 7: Logical plan caching for faster query startup.
/// </summary>
public class PlanCachingTests
{
    [ArrowRecord]
    public record CachingTestRecord
    {
        [ArrowArray(Name = "Id")]
        public int Id { get; init; }

        [ArrowArray(Name = "Value")]
        public int Value { get; init; }
    }

    private static FrozenArrow<CachingTestRecord> CreateTestData()
    {
        return new[]
        {
            new CachingTestRecord { Id = 1, Value = 100 },
            new CachingTestRecord { Id = 2, Value = 200 },
            new CachingTestRecord { Id = 3, Value = 300 },
        }.ToFrozenArrow();
    }

    [Fact]
    public void LogicalPlanCache_SameQuery_CachesAndReuses()
    {
        // Arrange
        var data = CreateTestData();
        var queryable = data.AsQueryable();
        var provider = (ArrowQueryProvider)queryable.Provider;
        provider.UseLogicalPlanExecution = true;
        provider.UseLogicalPlanCache = true;

        // Act - Execute same query twice
        var result1 = queryable.Where(x => x.Value > 100).Count();
        var stats1 = provider.GetLogicalPlanCacheStatistics();

        var result2 = queryable.Where(x => x.Value > 100).Count();
        var stats2 = provider.GetLogicalPlanCacheStatistics();

        // Assert
        Assert.Equal(2, result1);
        Assert.Equal(2, result2);
        Assert.Equal(0, stats1.Hits); // First query: miss
        Assert.Equal(1, stats1.Misses);
        Assert.Equal(1, stats2.Hits); // Second query: hit!
        Assert.Equal(1, stats2.Misses);
    }

    [Fact]
    public void LogicalPlanCache_DifferentQueries_CachesSeparately()
    {
        // Arrange
        var data = CreateTestData();
        var queryable = data.AsQueryable();
        var provider = (ArrowQueryProvider)queryable.Provider;
        provider.UseLogicalPlanExecution = true;
        provider.UseLogicalPlanCache = true;

        // Act
        var count1 = queryable.Where(x => x.Value > 100).Count();
        var count2 = queryable.Where(x => x.Value > 200).Count();
        var count3 = queryable.Where(x => x.Value > 100).Count(); // Repeat

        var stats = provider.GetLogicalPlanCacheStatistics();

        // Assert
        Assert.Equal(2, count1);
        Assert.Equal(1, count2);
        Assert.Equal(2, count3);
        Assert.Equal(1, stats.Hits); // Third query hit
        Assert.Equal(2, stats.Misses); // First two missed
        Assert.Equal(2, stats.Count); // Two cached
    }

    [Fact]
    public void LogicalPlanCache_Disabled_DoesNotCache()
    {
        // Arrange
        var data = CreateTestData();
        var queryable = data.AsQueryable();
        var provider = (ArrowQueryProvider)queryable.Provider;
        provider.UseLogicalPlanExecution = true;
        provider.UseLogicalPlanCache = false; // Disabled

        // Act
        var result1 = queryable.Where(x => x.Value > 100).Count();
        var result2 = queryable.Where(x => x.Value > 100).Count();

        var stats = provider.GetLogicalPlanCacheStatistics();

        // Assert
        Assert.Equal(2, result1);
        Assert.Equal(2, result2);
        Assert.Equal(0, stats.Hits);
        Assert.Equal(0, stats.Misses);
        Assert.Equal(0, stats.Count);
    }

    [Fact]
    public void LogicalPlanCache_Clear_RemovesAllEntries()
    {
        // Arrange
        var data = CreateTestData();
        var queryable = data.AsQueryable();
        var provider = (ArrowQueryProvider)queryable.Provider;
        provider.UseLogicalPlanExecution = true;
        provider.UseLogicalPlanCache = true;

        // Act
        queryable.Where(x => x.Value > 100).Count();
        var statsBefore = provider.GetLogicalPlanCacheStatistics();

        provider.ClearLogicalPlanCache();
        var statsAfter = provider.GetLogicalPlanCacheStatistics();

        // Assert
        Assert.Equal(1, statsBefore.Count);
        Assert.Equal(0, statsAfter.Count);
        Assert.Equal(0, statsAfter.Hits);
        Assert.Equal(0, statsAfter.Misses);
    }
}
