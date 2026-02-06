using FrozenArrow.Query;

namespace FrozenArrow.Tests.Correctness;

/// <summary>
/// Tests that verify optimization invariants - contracts that optimizations must maintain.
/// These tests ensure optimizations don't break correctness guarantees.
/// </summary>
public class OptimizationInvariantTests
{
    [ArrowRecord]
    public record InvariantTestRecord
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

    private static FrozenArrow<InvariantTestRecord> CreateTestData(int rowCount, int seed = 42)
    {
        var random = new Random(seed);
        var records = new List<InvariantTestRecord>(rowCount);

        for (int i = 0; i < rowCount; i++)
        {
            records.Add(new InvariantTestRecord
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
    public void Invariant_ZoneMapSkipping_NeverSkipsTooMuch(int rowCount)
    {
        // Invariant: Zone maps can skip chunks, but must never skip chunks with matching data
        
        // Arrange - Create sorted data optimal for zone maps
        var records = Enumerable.Range(0, rowCount)
            .Select(i => new InvariantTestRecord
            {
                Id = i,
                Value = i,
                Score = i / 100.0,
                IsActive = i % 2 == 0
            })
            .ToList();
        var data = records.ToFrozenArrow();

        // Act - Query that should match specific range
        var threshold = rowCount / 2;
        var results = data.AsQueryable().AllowFallback()
            .Where(x => x.Value > threshold)
            .ToList();

        var expectedCount = rowCount - threshold - 1;

        // Assert - Invariant: Zone maps cannot skip too much
        Assert.Equal(expectedCount, results.Count);
        Assert.All(results, r => Assert.True(r.Value > threshold));
    }

    [Theory]
    [InlineData(100_000)]
    public void Invariant_PredicateReordering_PreservesSemantics(int rowCount)
    {
        // Invariant: Predicate reordering is an optimization, not a semantic change
        
        // Arrange
        var data = CreateTestData(rowCount);

        // Act - Logically equivalent queries
        var query1 = data.AsQueryable().AllowFallback()
            .Where(x => x.Value > 500)
            .Where(x => x.Score > 50.0)
            .Count();

        var query2 = data.AsQueryable().AllowFallback()
            .Where(x => x.Score > 50.0)
            .Where(x => x.Value > 500)
            .Count();

        // Assert - Invariant: Reordering preserves semantics
        Assert.Equal(query1, query2);
    }

    [Theory]
    [InlineData(100_000)]
    public void Invariant_ParallelExecution_PreservesSingleThreadedSemantics(int rowCount)
    {
        // Invariant: Parallel execution must produce same results as single-threaded
        
        // Arrange
        var data = CreateTestData(rowCount);

        // Act - Execute multiple times (some may use parallel, some may not)
        var results = Enumerable.Range(0, 20)
            .Select(_ => data.AsQueryable().AllowFallback()
                .Where(x => x.Value > 500 && x.IsActive)
                .Count())
            .ToList();

        // Assert - Invariant: All executions produce same result
        var first = results[0];
        Assert.All(results, count => Assert.Equal(first, count));
    }

    [Theory]
    [InlineData(50_000)]
    public void Invariant_FusedOperations_MatchUnfusedResults(int rowCount)
    {
        // Invariant: Fused filter+aggregate must match separate operations
        
        // Arrange
        var data = CreateTestData(rowCount);

        // Act
        var fusedResult = data.AsQueryable().AllowFallback()
            .Where(x => x.Value > 500)
            .Sum(x => x.Value);

        var unfusedResult = data.AsQueryable().AllowFallback()
            .Where(x => x.Value > 500)
            .ToList()
            .Sum(x => x.Value);

        // Assert - Invariant: Fused == Unfused
        Assert.Equal(fusedResult, unfusedResult);
    }

    [Theory]
    [InlineData(100_000)]
    public void Invariant_BitmapOperations_PreserveSetSemantics(int rowCount)
    {
        // Invariant: Bitmap AND/OR operations follow set theory laws
        
        // Arrange
        var data = CreateTestData(rowCount);

        // Act - A ? B = B ? A (commutative)
        var countAandB = data.AsQueryable().AllowFallback()
            .Where(x => x.Value > 500)
            .Where(x => x.Score > 50.0)
            .Count();

        var countBandA = data.AsQueryable().AllowFallback()
            .Where(x => x.Score > 50.0)
            .Where(x => x.Value > 500)
            .Count();

        // Assert - Invariant: Commutativity
        Assert.Equal(countAandB, countBandA);
    }

    [Theory]
    [InlineData(100_000)]
    public void Invariant_SelectionBitmap_AccurateBitCounting(int rowCount)
    {
        // Invariant: SelectionBitmap.CountSet() must match actual count
        
        // Arrange
        var data = CreateTestData(rowCount);

        // Act
        var count = data.AsQueryable().AllowFallback()
            .Where(x => x.Value > 500)
            .Count();

        var list = data.AsQueryable().AllowFallback()
            .Where(x => x.Value > 500)
            .ToList();

        // Assert - Invariant: CountSet matches actual count
        Assert.Equal(count, list.Count);
    }

    [Theory]
    [InlineData(16383)]  // Just before chunk boundary
    [InlineData(16384)]  // Exactly at chunk boundary
    [InlineData(16385)]  // Just after chunk boundary
    public void Invariant_ChunkProcessing_NoDataLostAtBoundaries(int rowCount)
    {
        // Invariant: Chunk-based processing must not lose data at boundaries
        
        // Arrange
        var data = CreateTestData(rowCount);

        // Act
        var count = data.AsQueryable().AllowFallback()
            .Where(x => x.Value > 250 && x.Value < 750)
            .Count();

        var verifyCount = data.AsQueryable().AllowFallback()
            .ToList()
            .Count(x => x.Value > 250 && x.Value < 750);

        // Assert - Invariant: No data lost
        Assert.Equal(count, verifyCount);
    }

    [Theory]
    [InlineData(100_000)]
    public void Invariant_QueryPlanCaching_DoesNotAffectResults(int rowCount)
    {
        // Invariant: Query plan caching is transparent (same results with or without cache)
        
        // Arrange
        var data = CreateTestData(rowCount);

        // Act - First execution (populates cache)
        var firstResult = data.AsQueryable().AllowFallback()
            .Where(x => x.Value > 500 && x.IsActive)
            .Count();

        // Second execution (uses cache)
        var cachedResult = data.AsQueryable().AllowFallback()
            .Where(x => x.Value > 500 && x.IsActive)
            .Count();

        // Assert - Invariant: Caching is transparent
        Assert.Equal(firstResult, cachedResult);
    }

    [Theory]
    [InlineData(100_000)]
    public void Invariant_Aggregations_AssociativeProperty(int rowCount)
    {
        // Invariant: Sum should be associative (chunk-based sum == full sum)
        
        // Arrange
        var data = CreateTestData(rowCount);

        // Act
        var totalSum = data.AsQueryable().AllowFallback()
            .Where(x => x.IsActive)
            .Sum(x => x.Value);

        var verifySum = data.AsQueryable().AllowFallback()
            .Where(x => x.IsActive)
            .ToList()
            .Sum(x => x.Value);

        // Assert - Invariant: Associativity
        Assert.Equal(totalSum, verifySum);
    }

    [Theory]
    [InlineData(100_000)]
    public void Invariant_FilterMonotonicity_MoreRestrictiveFewerResults(int rowCount)
    {
        // Invariant: Adding predicates can only reduce results, never increase
        
        // Arrange
        var data = CreateTestData(rowCount);

        // Act
        var countOneFilter = data.AsQueryable().AllowFallback()
            .Where(x => x.Value > 500)
            .Count();

        var countTwoFilters = data.AsQueryable().AllowFallback()
            .Where(x => x.Value > 500)
            .Where(x => x.Score > 50.0)
            .Count();

        var countThreeFilters = data.AsQueryable().AllowFallback()
            .Where(x => x.Value > 500)
            .Where(x => x.Score > 50.0)
            .Where(x => x.IsActive)
            .Count();

        // Assert - Invariant: Monotonicity
        Assert.True(countOneFilter >= countTwoFilters,
            $"One filter ({countOneFilter}) should have >= results than two filters ({countTwoFilters})");
        Assert.True(countTwoFilters >= countThreeFilters,
            $"Two filters ({countTwoFilters}) should have >= results than three filters ({countThreeFilters})");
    }

    [Theory]
    [InlineData(100_000, 10)]
    public void Invariant_Determinism_SameInputSameOutput(int rowCount, int repetitions)
    {
        // Invariant: Same query on same data must always produce same results
        
        // Arrange
        var data = CreateTestData(rowCount);

        // Act - Execute same query multiple times
        var results = Enumerable.Range(0, repetitions)
            .Select(_ => data.AsQueryable().AllowFallback()
                .Where(x => x.Value > 500)
                .Where(x => x.Score > 50.0)
                .OrderBy(x => x.Id)
                .ToList())
            .ToList();

        // Assert - Invariant: Determinism
        var firstResult = results[0];
        foreach (var result in results.Skip(1))
        {
            Assert.Equal(firstResult.Count, result.Count);
            for (int i = 0; i < firstResult.Count; i++)
            {
                Assert.Equal(firstResult[i], result[i]);
            }
        }
    }

    [Theory]
    [InlineData(100_000)]
    public void Invariant_MemorySafety_NoBufferOverruns(int rowCount)
    {
        // Invariant: Operations must not access memory beyond allocated buffers
        
        // Arrange
        var data = CreateTestData(rowCount);

        // Act - Operations that involve bitmap/buffer manipulation
        var exceptions = new List<Exception>();
        
        try
        {
            _ = data.AsQueryable().AllowFallback().Where(x => x.Value > 500).Count();
            _ = data.AsQueryable().AllowFallback().Where(x => x.Score > 50.0).Any();
            _ = data.AsQueryable().AllowFallback().Where(x => x.IsActive).ToList();
        }
        catch (IndexOutOfRangeException ex)
        {
            exceptions.Add(ex);
        }
        catch (AccessViolationException ex)
        {
            exceptions.Add(ex);
        }

        // Assert - Invariant: No memory violations
        Assert.Empty(exceptions);
    }
}

