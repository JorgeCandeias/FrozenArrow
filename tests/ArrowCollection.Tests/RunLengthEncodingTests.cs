using Xunit;
using static ArrowCollection.Tests.ArrowCollectionTests;

namespace ArrowCollection.Tests;

/// <summary>
/// Tests for Run-Length Encoding (RLE) statistics detection.
/// Note: Actual RLE encoding requires Apache Arrow 13.0+. Until upgraded,
/// RLE candidates will use dictionary encoding as a fallback.
/// </summary>
public class RunLengthEncodingTests
{
    [Fact]
    public void SortedLowCardinalityStrings_ShouldBeRleCandidate()
    {
        // Arrange: Create pre-sorted items with very few runs
        var items = new List<SimpleItem>();
        
        // 10,000 "A" followed by 10,000 "B" followed by 10,000 "C" = 3 runs
        for (int i = 0; i < 10000; i++)
            items.Add(new SimpleItem { Id = i, Name = "A", Value = i });
        for (int i = 0; i < 10000; i++)
            items.Add(new SimpleItem { Id = i + 10000, Name = "B", Value = i });
        for (int i = 0; i < 10000; i++)
            items.Add(new SimpleItem { Id = i + 20000, Name = "C", Value = i });

        // Act
        using var collection = items.ToArrowCollection();

        // Assert
        Assert.NotNull(collection.BuildStatistics);
        var nameStats = collection.BuildStatistics.ColumnStatistics["Name"];
        
        // Verify statistics detected very low run ratio
        Assert.Equal(3, nameStats.RunCount); // Only 3 runs for 30,000 items
        Assert.Equal(30000, nameStats.TotalCount);
        Assert.True(nameStats.RunRatio < 0.001); // 3/30000 = 0.0001
        Assert.True(nameStats.ShouldUseRunLengthEncoding());
        Assert.Equal(ColumnEncoding.RunLengthEncoded, nameStats.RecommendedEncoding);

        // Verify data round-trips correctly
        var roundTripped = collection.ToList();
        Assert.Equal(30000, roundTripped.Count);
        Assert.Equal("A", roundTripped[0].Name);
        Assert.Equal("A", roundTripped[9999].Name);
        Assert.Equal("B", roundTripped[10000].Name);
        Assert.Equal("B", roundTripped[19999].Name);
        Assert.Equal("C", roundTripped[20000].Name);
        Assert.Equal("C", roundTripped[29999].Name);
    }

    [Fact]
    public void UnsortedData_ShouldNotBeRleCandidate()
    {
        // Arrange: Create random order items (many runs)
        var items = new List<SimpleItem>();
        var categories = new[] { "A", "B", "C" };
        
        // Alternating pattern creates maximum runs
        for (int i = 0; i < 10000; i++)
        {
            items.Add(new SimpleItem
            {
                Id = i,
                Name = categories[i % 3], // A, B, C, A, B, C...
                Value = i * 1.5
            });
        }

        // Act
        using var collection = items.ToArrowCollection();

        // Assert
        Assert.NotNull(collection.BuildStatistics);
        var nameStats = collection.BuildStatistics.ColumnStatistics["Name"];
        
        // Many runs (close to total count)
        Assert.True(nameStats.RunRatio > 0.5); // Most items are a new run
        Assert.False(nameStats.ShouldUseRunLengthEncoding());
        
        // Should prefer dictionary encoding instead (low cardinality)
        Assert.True(nameStats.ShouldUseDictionaryEncoding());
        Assert.Equal(ColumnEncoding.Dictionary, nameStats.RecommendedEncoding);

        // Verify data round-trips correctly
        var roundTripped = collection.ToList();
        Assert.Equal(10000, roundTripped.Count);
    }

    [Fact]
    public void SortedSparseIntegers_ShouldBeRleCandidate()
    {
        // Arrange: Sorted sparse data - mostly zeros with some values
        var items = new List<SimpleItem>();
        
        // 9000 zeros, then 1000 ones = 2 runs
        for (int i = 0; i < 9000; i++)
            items.Add(new SimpleItem { Id = 0, Name = $"Item_{i}", Value = i });
        for (int i = 0; i < 1000; i++)
            items.Add(new SimpleItem { Id = 1, Name = $"Item_{i + 9000}", Value = i });

        // Act
        using var collection = items.ToArrowCollection();

        // Assert
        Assert.NotNull(collection.BuildStatistics);
        var idStats = collection.BuildStatistics.ColumnStatistics["Id"];
        
        Assert.Equal(2, idStats.RunCount);
        Assert.Equal(10000, idStats.TotalCount);
        Assert.True(idStats.ShouldUseRunLengthEncoding());
        Assert.Equal(ColumnEncoding.RunLengthEncoded, idStats.RecommendedEncoding);

        // Verify data round-trips
        var roundTripped = collection.ToList();
        Assert.Equal(0, roundTripped[0].Id);
        Assert.Equal(0, roundTripped[8999].Id);
        Assert.Equal(1, roundTripped[9000].Id);
        Assert.Equal(1, roundTripped[9999].Id);
    }

    [Fact]
    public void BuildStatistics_ReportsRleCandidates()
    {
        // Arrange: Mix of sorted (RLE-friendly) and random data
        var items = new List<SimpleItem>();
        
        // Id: sorted (RLE candidate) - 5000 zeros, 5000 ones
        // Name: sorted (RLE candidate) - 5000 "First", 5000 "Second"
        // Value: high cardinality (not RLE candidate)
        for (int i = 0; i < 5000; i++)
        {
            items.Add(new SimpleItem { Id = 0, Name = "First", Value = i * 0.01 });
        }
        for (int i = 0; i < 5000; i++)
        {
            items.Add(new SimpleItem { Id = 1, Name = "Second", Value = (i + 5000) * 0.01 });
        }

        // Act
        using var collection = items.ToArrowCollection();

        // Assert
        Assert.NotNull(collection.BuildStatistics);
        
        var rleCandidates = collection.BuildStatistics.GetRunLengthEncodingCandidates().ToList();
        Assert.Equal(2, rleCandidates.Count); // Id and Name
        Assert.Contains(rleCandidates, c => c.ColumnName == "Id");
        Assert.Contains(rleCandidates, c => c.ColumnName == "Name");
        Assert.DoesNotContain(rleCandidates, c => c.ColumnName == "Value");
    }

    [Fact]
    public void PreSortedByColumn_ShowsRleBenefit()
    {
        // Arrange: Create items then sort by Name
        var items = new List<SimpleItem>();
        var categories = new[] { "Electronics", "Books", "Clothing", "Food", "Other" };
        
        for (int i = 0; i < 10000; i++)
        {
            items.Add(new SimpleItem
            {
                Id = i,
                Name = categories[i % 5],
                Value = i * 1.5
            });
        }

        // Act: Sort before converting to ArrowCollection
        var sortedItems = items.OrderBy(x => x.Name).ToList();
        using var collection = sortedItems.ToArrowCollection();

        // Assert
        Assert.NotNull(collection.BuildStatistics);
        var nameStats = collection.BuildStatistics.ColumnStatistics["Name"];
        
        // After sorting, we should have only 5 runs (one per category)
        Assert.Equal(5, nameStats.RunCount);
        Assert.Equal(10000, nameStats.TotalCount);
        Assert.True(nameStats.ShouldUseRunLengthEncoding());
        
        // Verify correct ordering
        var roundTripped = collection.ToList();
        Assert.Equal("Books", roundTripped[0].Name); // Alphabetically first
        Assert.Equal("Other", roundTripped[9999].Name); // Alphabetically last after Food
    }
}
