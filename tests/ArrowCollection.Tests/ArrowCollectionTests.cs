namespace ArrowCollection.Tests;

public class ArrowCollectionTests
{
    [ArrowRecord]
    public class SimpleItem
    {
        [ArrowArray]
        public int Id { get; set; }
        [ArrowArray]
        public string Name { get; set; } = string.Empty;
        [ArrowArray]
        public double Value { get; set; }
    }

    [ArrowRecord]
    public class ComplexItem
    {
        [ArrowArray]
        public int IntValue { get; set; }
        [ArrowArray]
        public long LongValue { get; set; }
        [ArrowArray]
        public short ShortValue { get; set; }
        [ArrowArray]
        public byte ByteValue { get; set; }
        [ArrowArray]
        public float FloatValue { get; set; }
        [ArrowArray]
        public double DoubleValue { get; set; }
        [ArrowArray]
        public bool BoolValue { get; set; }
        [ArrowArray]
        public string? StringValue { get; set; }
        [ArrowArray]
        public DateTime DateTimeValue { get; set; }
    }

    [ArrowRecord]
    public class NullableItem
    {
        [ArrowArray]
        public int? NullableInt { get; set; }
        [ArrowArray]
        public string? NullableString { get; set; }
        [ArrowArray]
        public DateTime? NullableDateTime { get; set; }
    }

    [Fact]
    public void ToArrowCollection_WithSimpleItems_CanEnumerateAll()
    {
        // Arrange
        var items = new[]
        {
            new SimpleItem { Id = 1, Name = "Item 1", Value = 10.5 },
            new SimpleItem { Id = 2, Name = "Item 2", Value = 20.5 },
            new SimpleItem { Id = 3, Name = "Item 3", Value = 30.5 }
        };

        // Act
        using var collection = items.ToArrowCollection();

        // Assert
        Assert.Equal(3, collection.Count);

        var result = collection.ToList();
        Assert.Equal(3, result.Count);

        Assert.Equal(1, result[0].Id);
        Assert.Equal("Item 1", result[0].Name);
        Assert.Equal(10.5, result[0].Value);

        Assert.Equal(2, result[1].Id);
        Assert.Equal("Item 2", result[1].Name);
        Assert.Equal(20.5, result[1].Value);

        Assert.Equal(3, result[2].Id);
        Assert.Equal("Item 3", result[2].Name);
        Assert.Equal(30.5, result[2].Value);
    }

    [Fact]
    public void ToArrowCollection_WithComplexTypes_PreservesAllDataTypes()
    {
        // Arrange
        var items = new[]
        {
            new ComplexItem
            {
                IntValue = 100,
                LongValue = 1000000L,
                ShortValue = 10,
                ByteValue = 5,
                FloatValue = 1.5f,
                DoubleValue = 2.5,
                BoolValue = true,
                StringValue = "Test",
                DateTimeValue = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc)
            },
            new ComplexItem
            {
                IntValue = 200,
                LongValue = 2000000L,
                ShortValue = 20,
                ByteValue = 10,
                FloatValue = 3.5f,
                DoubleValue = 4.5,
                BoolValue = false,
                StringValue = "Test2",
                DateTimeValue = new DateTime(2024, 6, 20, 15, 45, 0, DateTimeKind.Utc)
            }
        };

        // Act
        using var collection = items.ToArrowCollection();

        // Assert
        var result = collection.ToList();
        Assert.Equal(2, result.Count);

        var first = result[0];
        Assert.Equal(100, first.IntValue);
        Assert.Equal(1000000L, first.LongValue);
        Assert.Equal(10, first.ShortValue);
        Assert.Equal((byte)5, first.ByteValue);
        Assert.Equal(1.5f, first.FloatValue, precision: 5);
        Assert.Equal(2.5, first.DoubleValue, precision: 5);
        Assert.True(first.BoolValue);
        Assert.Equal("Test", first.StringValue);
        Assert.Equal(new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc), first.DateTimeValue);
    }

    [Fact]
    public void ToArrowCollection_WithNullableValues_HandlesNullsCorrectly()
    {
        // Arrange
        var items = new[]
        {
            new NullableItem { NullableInt = 42, NullableString = "Test", NullableDateTime = DateTime.UtcNow },
            new NullableItem { NullableInt = null, NullableString = null, NullableDateTime = null },
            new NullableItem { NullableInt = 100, NullableString = "Another", NullableDateTime = new DateTime(2024, 1, 1) }
        };

        // Act
        using var collection = items.ToArrowCollection();

        // Assert
        var result = collection.ToList();
        Assert.Equal(3, result.Count);

        Assert.Equal(42, result[0].NullableInt);
        Assert.Equal("Test", result[0].NullableString);
        Assert.NotNull(result[0].NullableDateTime);

        Assert.Null(result[1].NullableInt);
        Assert.Null(result[1].NullableString);
        Assert.Null(result[1].NullableDateTime);

        Assert.Equal(100, result[2].NullableInt);
        Assert.Equal("Another", result[2].NullableString);
        Assert.NotNull(result[2].NullableDateTime);
    }

    [Fact]
    public void ToArrowCollection_WithEmptyCollection_ReturnsEmptyColly()
    {
        // Arrange
        var items = Array.Empty<SimpleItem>();

        // Act
        using var collection = items.ToArrowCollection();

        // Assert
        Assert.Equal(0, collection.Count);
        Assert.Empty(collection);
    }

    [Fact]
    public void ToArrowCollection_CanEnumerateMultipleTimes()
    {
        // Arrange
        var items = new[]
        {
            new SimpleItem { Id = 1, Name = "Item 1", Value = 10.5 },
            new SimpleItem { Id = 2, Name = "Item 2", Value = 20.5 }
        };

        // Act
        using var collection = items.ToArrowCollection();

        // Assert - enumerate twice
        var firstEnumeration = collection.ToList();
        var secondEnumeration = collection.ToList();

        Assert.Equal(2, firstEnumeration.Count);
        Assert.Equal(2, secondEnumeration.Count);

        Assert.Equal(firstEnumeration[0].Id, secondEnumeration[0].Id);
        Assert.Equal(firstEnumeration[1].Id, secondEnumeration[1].Id);
    }

    [Fact]
    public void ToArrowCollection_WithNullSource_ThrowsArgumentNullException()
    {
        // Arrange
        IEnumerable<SimpleItem>? items = null;

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => items!.ToArrowCollection());
    }

    [Fact]
    public void ToArrowCollection_IsImmutable_OriginalDataChangesDoNotAffectColly()
    {
        // Arrange
        var items = new List<SimpleItem>
        {
            new() { Id = 1, Name = "Item 1", Value = 10.5 },
            new() { Id = 2, Name = "Item 2", Value = 20.5 }
        };

        // Act
        using var collection = items.ToArrowCollection();

        // Modify original data
        items[0].Name = "Modified";
        items.Add(new SimpleItem { Id = 3, Name = "Item 3", Value = 30.5 });

        // Assert - collection should contain original data
        var result = collection.ToList();
        Assert.Equal(2, result.Count); // Still 2 items, not 3
        Assert.Equal("Item 1", result[0].Name); // Name not changed
    }

    [Fact]
    public void ToArrowCollection_WithLargeDataset_HandlesCorrectly()
    {
        // Arrange
        var items = Enumerable.Range(1, 1000).Select(i => new SimpleItem
        {
            Id = i,
            Name = $"Item {i}",
            Value = i * 1.5
        }).ToList();

        // Act
        using var collection = items.ToArrowCollection();

        // Assert
        Assert.Equal(1000, collection.Count);

        var result = collection.ToList();
        Assert.Equal(1000, result.Count);

        // Spot check a few items
        Assert.Equal(1, result[0].Id);
        Assert.Equal("Item 1", result[0].Name);
        Assert.Equal(1.5, result[0].Value);

        Assert.Equal(500, result[499].Id);
        Assert.Equal("Item 500", result[499].Name);
        Assert.Equal(750.0, result[499].Value);

        Assert.Equal(1000, result[999].Id);
        Assert.Equal("Item 1000", result[999].Name);
        Assert.Equal(1500.0, result[999].Value);
    }

    [Fact]
    public void Dispose_PreventsEnumeration()
    {
        // Arrange
        var items = new[]
        {
            new SimpleItem { Id = 1, Name = "Item 1", Value = 10.5 }
        };

        var collection = items.ToArrowCollection();

        // Act
        collection.Dispose();

        // Assert
        Assert.Throws<ObjectDisposedException>(() => collection.GetEnumerator());
    }

    [Fact]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        // Arrange
        var items = new[]
        {
            new SimpleItem { Id = 1, Name = "Item 1", Value = 10.5 }
        };

        var collection = items.ToArrowCollection();

        // Act & Assert - should not throw
        collection.Dispose();
        collection.Dispose();
        collection.Dispose();
    }
}


