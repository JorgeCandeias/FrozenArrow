using Xunit;

namespace Colly.Tests;

public class CollyTests
{
    public class SimpleItem
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public double Value { get; set; }
    }

    public class ComplexItem
    {
        public int IntValue { get; set; }
        public long LongValue { get; set; }
        public short ShortValue { get; set; }
        public byte ByteValue { get; set; }
        public float FloatValue { get; set; }
        public double DoubleValue { get; set; }
        public bool BoolValue { get; set; }
        public string? StringValue { get; set; }
        public DateTime DateTimeValue { get; set; }
    }

    public class NullableItem
    {
        public int? NullableInt { get; set; }
        public string? NullableString { get; set; }
        public DateTime? NullableDateTime { get; set; }
    }

    [Fact]
    public void ToColly_WithSimpleItems_CanEnumerateAll()
    {
        // Arrange
        var items = new[]
        {
            new SimpleItem { Id = 1, Name = "Item 1", Value = 10.5 },
            new SimpleItem { Id = 2, Name = "Item 2", Value = 20.5 },
            new SimpleItem { Id = 3, Name = "Item 3", Value = 30.5 }
        };

        // Act
        var colly = items.ToColly();

        // Assert
        Assert.Equal(3, colly.Count);

        var result = colly.ToList();
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
    public void ToColly_WithComplexTypes_PreservesAllDataTypes()
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
        var colly = items.ToColly();

        // Assert
        var result = colly.ToList();
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
    public void ToColly_WithNullableValues_HandlesNullsCorrectly()
    {
        // Arrange
        var items = new[]
        {
            new NullableItem { NullableInt = 42, NullableString = "Test", NullableDateTime = DateTime.UtcNow },
            new NullableItem { NullableInt = null, NullableString = null, NullableDateTime = null },
            new NullableItem { NullableInt = 100, NullableString = "Another", NullableDateTime = new DateTime(2024, 1, 1) }
        };

        // Act
        var colly = items.ToColly();

        // Assert
        var result = colly.ToList();
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
    public void ToColly_WithEmptyCollection_ReturnsEmptyColly()
    {
        // Arrange
        var items = Array.Empty<SimpleItem>();

        // Act
        var colly = items.ToColly();

        // Assert
        Assert.Equal(0, colly.Count);
        Assert.Empty(colly);
    }

    [Fact]
    public void ToColly_CanEnumerateMultipleTimes()
    {
        // Arrange
        var items = new[]
        {
            new SimpleItem { Id = 1, Name = "Item 1", Value = 10.5 },
            new SimpleItem { Id = 2, Name = "Item 2", Value = 20.5 }
        };

        // Act
        var colly = items.ToColly();

        // Assert - enumerate twice
        var firstEnumeration = colly.ToList();
        var secondEnumeration = colly.ToList();

        Assert.Equal(2, firstEnumeration.Count);
        Assert.Equal(2, secondEnumeration.Count);

        Assert.Equal(firstEnumeration[0].Id, secondEnumeration[0].Id);
        Assert.Equal(firstEnumeration[1].Id, secondEnumeration[1].Id);
    }

    [Fact]
    public void ToColly_WithNullSource_ThrowsArgumentNullException()
    {
        // Arrange
        IEnumerable<SimpleItem>? items = null;

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => items!.ToColly());
    }

    [Fact]
    public void ToColly_IsImmutable_OriginalDataChangesDoNotAffectColly()
    {
        // Arrange
        var items = new List<SimpleItem>
        {
            new SimpleItem { Id = 1, Name = "Item 1", Value = 10.5 },
            new SimpleItem { Id = 2, Name = "Item 2", Value = 20.5 }
        };

        // Act
        var colly = items.ToColly();

        // Modify original data
        items[0].Name = "Modified";
        items.Add(new SimpleItem { Id = 3, Name = "Item 3", Value = 30.5 });

        // Assert - Colly should contain original data
        var result = colly.ToList();
        Assert.Equal(2, result.Count); // Still 2 items, not 3
        Assert.Equal("Item 1", result[0].Name); // Name not changed
    }

    [Fact]
    public void ToColly_WithLargeDataset_HandlesCorrectly()
    {
        // Arrange
        var items = Enumerable.Range(1, 1000).Select(i => new SimpleItem
        {
            Id = i,
            Name = $"Item {i}",
            Value = i * 1.5
        }).ToList();

        // Act
        var colly = items.ToColly();

        // Assert
        Assert.Equal(1000, colly.Count);

        var result = colly.ToList();
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
}
