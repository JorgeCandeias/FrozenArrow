namespace FrozenArrow.Tests;

public class FrozenArrowTests
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
        public Half HalfValue { get; set; }
        [ArrowArray]
        public bool BoolValue { get; set; }
        [ArrowArray]
        public string? StringValue { get; set; }
        [ArrowArray]
        public byte[] BinaryValue { get; set; } = [];
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
        [ArrowArray]
        public Half? NullableHalf { get; set; }
        [ArrowArray]
        public byte[]? NullableBinary { get; set; }
    }

    [Fact]
    public void ToFrozenArrow_WithSimpleItems_CanEnumerateAll()
    {
        // Arrange
        var items = new[]
        {
            new SimpleItem { Id = 1, Name = "Item 1", Value = 10.5 },
            new SimpleItem { Id = 2, Name = "Item 2", Value = 20.5 },
            new SimpleItem { Id = 3, Name = "Item 3", Value = 30.5 }
        };

        // Act
        using var collection = items.ToFrozenArrow();

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
    public void ToFrozenArrow_WithComplexTypes_PreservesAllDataTypes()
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
                HalfValue = (Half)1.25f,
                BoolValue = true,
                StringValue = "Test",
                BinaryValue = [1, 2, 3, 4, 5],
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
                HalfValue = (Half)2.75f,
                BoolValue = false,
                StringValue = "Test2",
                BinaryValue = [10, 20, 30],
                DateTimeValue = new DateTime(2024, 6, 20, 15, 45, 0, DateTimeKind.Utc)
            }
        };

        // Act
        using var collection = items.ToFrozenArrow();

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
        Assert.Equal((Half)1.25f, first.HalfValue);
        Assert.True(first.BoolValue);
        Assert.Equal("Test", first.StringValue);
        Assert.Equal(new byte[] { 1, 2, 3, 4, 5 }, first.BinaryValue);
        Assert.Equal(new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc), first.DateTimeValue);

        var second = result[1];
        Assert.Equal((Half)2.75f, second.HalfValue);
        Assert.Equal(new byte[] { 10, 20, 30 }, second.BinaryValue);
    }

    [Fact]
    public void ToFrozenArrow_WithNullableValues_HandlesNullsCorrectly()
    {
        // Arrange
        var items = new[]
        {
            new NullableItem { NullableInt = 42, NullableString = "Test", NullableDateTime = DateTime.UtcNow, NullableHalf = (Half)1.5f, NullableBinary = [1, 2, 3] },
            new NullableItem { NullableInt = null, NullableString = null, NullableDateTime = null, NullableHalf = null, NullableBinary = null },
            new NullableItem { NullableInt = 100, NullableString = "Another", NullableDateTime = new DateTime(2024, 1, 1), NullableHalf = (Half)2.5f, NullableBinary = [4, 5] }
        };

        // Act
        using var collection = items.ToFrozenArrow();

        // Assert
        var result = collection.ToList();
        Assert.Equal(3, result.Count);

        Assert.Equal(42, result[0].NullableInt);
        Assert.Equal("Test", result[0].NullableString);
        Assert.NotNull(result[0].NullableDateTime);
        Assert.Equal((Half)1.5f, result[0].NullableHalf);
        Assert.Equal(new byte[] { 1, 2, 3 }, result[0].NullableBinary);

        Assert.Null(result[1].NullableInt);
        Assert.Null(result[1].NullableString);
        Assert.Null(result[1].NullableDateTime);
        Assert.Null(result[1].NullableHalf);
        Assert.Null(result[1].NullableBinary);

        Assert.Equal(100, result[2].NullableInt);
        Assert.Equal("Another", result[2].NullableString);
        Assert.NotNull(result[2].NullableDateTime);
        Assert.Equal((Half)2.5f, result[2].NullableHalf);
        Assert.Equal(new byte[] { 4, 5 }, result[2].NullableBinary);
    }

    [Fact]
    public void ToFrozenArrow_WithEmptyCollection_ReturnsEmptyFrozenArrow()
    {
        // Arrange
        var items = Array.Empty<SimpleItem>();

        // Act
        using var collection = items.ToFrozenArrow();

        // Assert
        Assert.Equal(0, collection.Count);
        Assert.Empty(collection);
    }

    [Fact]
    public void ToFrozenArrow_CanEnumerateMultipleTimes()
    {
        // Arrange
        var items = new[]
        {
            new SimpleItem { Id = 1, Name = "Item 1", Value = 10.5 },
            new SimpleItem { Id = 2, Name = "Item 2", Value = 20.5 }
        };

        // Act
        using var collection = items.ToFrozenArrow();

        // Assert - enumerate twice
        var firstEnumeration = collection.ToList();
        var secondEnumeration = collection.ToList();

        Assert.Equal(2, firstEnumeration.Count);
        Assert.Equal(2, secondEnumeration.Count);

        Assert.Equal(firstEnumeration[0].Id, secondEnumeration[0].Id);
        Assert.Equal(firstEnumeration[1].Id, secondEnumeration[1].Id);
    }

    [Fact]
    public void ToFrozenArrow_WithNullSource_ThrowsArgumentNullException()
    {
        // Arrange
        IEnumerable<SimpleItem>? items = null;

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => items!.ToFrozenArrow());
    }

    [Fact]
    public void ToFrozenArrow_IsImmutable_OriginalDataChangesDoNotAffectFrozenArrow()
    {
        // Arrange
        var items = new List<SimpleItem>
        {
            new() { Id = 1, Name = "Item 1", Value = 10.5 },
            new() { Id = 2, Name = "Item 2", Value = 20.5 }
        };

        // Act
        using var collection = items.ToFrozenArrow();

        // Modify original data
        items[0].Name = "Modified";
        items.Add(new SimpleItem { Id = 3, Name = "Item 3", Value = 30.5 });

        // Assert - collection should contain original data
        var result = collection.ToList();
        Assert.Equal(2, result.Count); // Still 2 items, not 3
        Assert.Equal("Item 1", result[0].Name); // Name not changed
    }

    [Fact]
    public void ToFrozenArrow_WithLargeDataset_HandlesCorrectly()
    {
        // Arrange
        var items = Enumerable.Range(1, 1000).Select(i => new SimpleItem
        {
            Id = i,
            Name = $"Item {i}",
            Value = i * 1.5
        }).ToList();

        // Act
        using var collection = items.ToFrozenArrow();

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

        var collection = items.ToFrozenArrow();

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

        var collection = items.ToFrozenArrow();

        // Act & Assert - should not throw
        collection.Dispose();
        collection.Dispose();
        collection.Dispose();
    }
}

/// <summary>
/// Tests for struct support in FrozenArrow.
/// </summary>
public class FrozenArrowStructTests
{
    #region Test Structs

    /// <summary>
    /// Simple mutable struct for basic tests.
    /// </summary>
    [ArrowRecord]
    public struct SimpleStruct
    {
        [ArrowArray]
        public int Id { get; set; }
        [ArrowArray]
        public string Name { get; set; }
        [ArrowArray]
        public double Value { get; set; }
    }

    /// <summary>
    /// Readonly struct - tests that IL generation works with readonly structs.
    /// </summary>
    [ArrowRecord]
    public readonly struct ReadonlyStruct
    {
        [ArrowArray]
        public int Id { get; init; }
        [ArrowArray]
        public string Name { get; init; }
        [ArrowArray]
        public double Value { get; init; }
    }

    /// <summary>
    /// Struct with all supported data types.
    /// </summary>
    [ArrowRecord]
    public struct ComplexStruct
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
        public Half HalfValue { get; set; }
        [ArrowArray]
        public bool BoolValue { get; set; }
        [ArrowArray]
        public string? StringValue { get; set; }
        [ArrowArray]
        public byte[] BinaryValue { get; set; }
        [ArrowArray]
        public DateTime DateTimeValue { get; set; }
    }

    /// <summary>
    /// Struct with nullable fields.
    /// </summary>
    [ArrowRecord]
    public struct NullableStruct
    {
        [ArrowArray]
        public int? NullableInt { get; set; }
        [ArrowArray]
        public string? NullableString { get; set; }
        [ArrowArray]
        public DateTime? NullableDateTime { get; set; }
        [ArrowArray]
        public Half? NullableHalf { get; set; }
        [ArrowArray]
        public byte[]? NullableBinary { get; set; }
    }

    /// <summary>
    /// Readonly struct with nullable fields.
    /// </summary>
    [ArrowRecord]
    public readonly struct ReadonlyNullableStruct
    {
        [ArrowArray]
        public int? NullableInt { get; init; }
        [ArrowArray]
        public string? NullableString { get; init; }
        [ArrowArray]
        public DateTime? NullableDateTime { get; init; }
        [ArrowArray]
        public Half? NullableHalf { get; init; }
        [ArrowArray]
        public byte[]? NullableBinary { get; init; }
    }

    /// <summary>
    /// Struct using fields instead of properties.
    /// </summary>
    [ArrowRecord]
    public struct FieldStruct
    {
        [ArrowArray(Name = "Id")]
        public int Id;
        [ArrowArray(Name = "Name")]
        public string Name;
        [ArrowArray(Name = "Value")]
        public double Value;
    }

    #endregion

    #region Basic Struct Tests

    [Fact]
    public void ToFrozenArrow_WithSimpleStruct_CanEnumerateAll()
    {
        // Arrange
        var items = new[]
        {
            new SimpleStruct { Id = 1, Name = "Item 1", Value = 10.5 },
            new SimpleStruct { Id = 2, Name = "Item 2", Value = 20.5 },
            new SimpleStruct { Id = 3, Name = "Item 3", Value = 30.5 }
        };

        // Act
        using var collection = items.ToFrozenArrow();

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
    public void ToFrozenArrow_WithReadonlyStruct_CanEnumerateAll()
    {
        // Arrange
        var items = new[]
        {
            new ReadonlyStruct { Id = 1, Name = "Readonly 1", Value = 100.5 },
            new ReadonlyStruct { Id = 2, Name = "Readonly 2", Value = 200.5 },
            new ReadonlyStruct { Id = 3, Name = "Readonly 3", Value = 300.5 }
        };

        // Act
        using var collection = items.ToFrozenArrow();

        // Assert
        Assert.Equal(3, collection.Count);

        var result = collection.ToList();
        Assert.Equal(3, result.Count);

        Assert.Equal(1, result[0].Id);
        Assert.Equal("Readonly 1", result[0].Name);
        Assert.Equal(100.5, result[0].Value);

        Assert.Equal(2, result[1].Id);
        Assert.Equal("Readonly 2", result[1].Name);
        Assert.Equal(200.5, result[1].Value);

        Assert.Equal(3, result[2].Id);
        Assert.Equal("Readonly 3", result[2].Name);
        Assert.Equal(300.5, result[2].Value);
    }

    #endregion

    #region Complex Type Tests

    [Fact]
    public void ToFrozenArrow_WithComplexStruct_PreservesAllDataTypes()
    {
        // Arrange
        var items = new[]
        {
            new ComplexStruct
            {
                IntValue = 100,
                LongValue = 1000000L,
                ShortValue = 10,
                ByteValue = 5,
                FloatValue = 1.5f,
                DoubleValue = 2.5,
                HalfValue = (Half)1.25f,
                BoolValue = true,
                StringValue = "Test",
                BinaryValue = [1, 2, 3, 4, 5],
                DateTimeValue = new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc)
            },
            new ComplexStruct
            {
                IntValue = 200,
                LongValue = 2000000L,
                ShortValue = 20,
                ByteValue = 10,
                FloatValue = 3.5f,
                DoubleValue = 4.5,
                HalfValue = (Half)2.75f,
                BoolValue = false,
                StringValue = "Test2",
                BinaryValue = [10, 20, 30],
                DateTimeValue = new DateTime(2024, 6, 20, 15, 45, 0, DateTimeKind.Utc)
            }
        };

        // Act
        using var collection = items.ToFrozenArrow();

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
        Assert.Equal((Half)1.25f, first.HalfValue);
        Assert.True(first.BoolValue);
        Assert.Equal("Test", first.StringValue);
        Assert.Equal(new byte[] { 1, 2, 3, 4, 5 }, first.BinaryValue);
        Assert.Equal(new DateTime(2024, 1, 15, 10, 30, 0, DateTimeKind.Utc), first.DateTimeValue);

        var second = result[1];
        Assert.Equal((Half)2.75f, second.HalfValue);
        Assert.Equal(new byte[] { 10, 20, 30 }, second.BinaryValue);
    }

    #endregion

    #region Nullable Field Tests

    [Fact]
    public void ToFrozenArrow_WithNullableStruct_HandlesNullsCorrectly()
    {
        // Arrange
        var items = new[]
        {
            new NullableStruct { NullableInt = 42, NullableString = "Test", NullableDateTime = DateTime.UtcNow, NullableHalf = (Half)1.5f, NullableBinary = [1, 2, 3] },
            new NullableStruct { NullableInt = null, NullableString = null, NullableDateTime = null, NullableHalf = null, NullableBinary = null },
            new NullableStruct { NullableInt = 100, NullableString = "Another", NullableDateTime = new DateTime(2024, 1, 1), NullableHalf = (Half)2.5f, NullableBinary = [4, 5] }
        };

        // Act
        using var collection = items.ToFrozenArrow();

        // Assert
        var result = collection.ToList();
        Assert.Equal(3, result.Count);

        Assert.Equal(42, result[0].NullableInt);
        Assert.Equal("Test", result[0].NullableString);
        Assert.NotNull(result[0].NullableDateTime);
        Assert.Equal((Half)1.5f, result[0].NullableHalf);
        Assert.Equal(new byte[] { 1, 2, 3 }, result[0].NullableBinary);

        Assert.Null(result[1].NullableInt);
        Assert.Null(result[1].NullableString);
        Assert.Null(result[1].NullableDateTime);
        Assert.Null(result[1].NullableHalf);
        Assert.Null(result[1].NullableBinary);

        Assert.Equal(100, result[2].NullableInt);
        Assert.Equal("Another", result[2].NullableString);
        Assert.NotNull(result[2].NullableDateTime);
        Assert.Equal((Half)2.5f, result[2].NullableHalf);
        Assert.Equal(new byte[] { 4, 5 }, result[2].NullableBinary);
    }

    [Fact]
    public void ToFrozenArrow_WithReadonlyNullableStruct_HandlesNullsCorrectly()
    {
        // Arrange
        var items = new[]
        {
            new ReadonlyNullableStruct { NullableInt = 42, NullableString = "Test", NullableDateTime = DateTime.UtcNow, NullableHalf = (Half)1.5f, NullableBinary = [1, 2, 3] },
            new ReadonlyNullableStruct { NullableInt = null, NullableString = null, NullableDateTime = null, NullableHalf = null, NullableBinary = null },
            new ReadonlyNullableStruct { NullableInt = 100, NullableString = "Another", NullableDateTime = new DateTime(2024, 1, 1), NullableHalf = (Half)2.5f, NullableBinary = [4, 5] }
        };

        // Act
        using var collection = items.ToFrozenArrow();

        // Assert
        var result = collection.ToList();
        Assert.Equal(3, result.Count);

        Assert.Equal(42, result[0].NullableInt);
        Assert.Equal("Test", result[0].NullableString);
        Assert.NotNull(result[0].NullableDateTime);
        Assert.Equal((Half)1.5f, result[0].NullableHalf);
        Assert.Equal(new byte[] { 1, 2, 3 }, result[0].NullableBinary);

        Assert.Null(result[1].NullableInt);
        Assert.Null(result[1].NullableString);
        Assert.Null(result[1].NullableDateTime);
        Assert.Null(result[1].NullableHalf);
        Assert.Null(result[1].NullableBinary);

        Assert.Equal(100, result[2].NullableInt);
        Assert.Equal("Another", result[2].NullableString);
        Assert.NotNull(result[2].NullableDateTime);
        Assert.Equal((Half)2.5f, result[2].NullableHalf);
        Assert.Equal(new byte[] { 4, 5 }, result[2].NullableBinary);
    }

    #endregion

    #region Empty Collection Tests

    [Fact]
    public void ToFrozenArrow_WithEmptyStructCollection_ReturnsEmpty()
    {
        // Arrange
        var items = Array.Empty<SimpleStruct>();

        // Act
        using var collection = items.ToFrozenArrow();

        // Assert
        Assert.Equal(0, collection.Count);
        Assert.Empty(collection);
    }

    [Fact]
    public void ToFrozenArrow_WithEmptyReadonlyStructCollection_ReturnsEmpty()
    {
        // Arrange
        var items = Array.Empty<ReadonlyStruct>();

        // Act
        using var collection = items.ToFrozenArrow();

        // Assert
        Assert.Equal(0, collection.Count);
        Assert.Empty(collection);
    }

    #endregion

    #region Multiple Enumeration Tests

    [Fact]
    public void ToFrozenArrow_WithStruct_CanEnumerateMultipleTimes()
    {
        // Arrange
        var items = new[]
        {
            new SimpleStruct { Id = 1, Name = "Item 1", Value = 10.5 },
            new SimpleStruct { Id = 2, Name = "Item 2", Value = 20.5 }
        };

        // Act
        using var collection = items.ToFrozenArrow();

        // Assert - enumerate twice
        var firstEnumeration = collection.ToList();
        var secondEnumeration = collection.ToList();

        Assert.Equal(2, firstEnumeration.Count);
        Assert.Equal(2, secondEnumeration.Count);

        Assert.Equal(firstEnumeration[0].Id, secondEnumeration[0].Id);
        Assert.Equal(firstEnumeration[1].Id, secondEnumeration[1].Id);
    }

    [Fact]
    public void ToFrozenArrow_WithReadonlyStruct_CanEnumerateMultipleTimes()
    {
        // Arrange
        var items = new[]
        {
            new ReadonlyStruct { Id = 1, Name = "Item 1", Value = 10.5 },
            new ReadonlyStruct { Id = 2, Name = "Item 2", Value = 20.5 }
        };

        // Act
        using var collection = items.ToFrozenArrow();

        // Assert - enumerate twice
        var firstEnumeration = collection.ToList();
        var secondEnumeration = collection.ToList();

        Assert.Equal(2, firstEnumeration.Count);
        Assert.Equal(2, secondEnumeration.Count);

        Assert.Equal(firstEnumeration[0].Id, secondEnumeration[0].Id);
        Assert.Equal(firstEnumeration[1].Id, secondEnumeration[1].Id);
    }

    #endregion

    #region Field-based Struct Tests

    [Fact]
    public void ToFrozenArrow_WithFieldStruct_CanEnumerateAll()
    {
        // Arrange
        var items = new[]
        {
            new FieldStruct { Id = 1, Name = "Field 1", Value = 10.5 },
            new FieldStruct { Id = 2, Name = "Field 2", Value = 20.5 },
            new FieldStruct { Id = 3, Name = "Field 3", Value = 30.5 }
        };

        // Act
        using var collection = items.ToFrozenArrow();

        // Assert
        Assert.Equal(3, collection.Count);

        var result = collection.ToList();
        Assert.Equal(3, result.Count);

        Assert.Equal(1, result[0].Id);
        Assert.Equal("Field 1", result[0].Name);
        Assert.Equal(10.5, result[0].Value);

        Assert.Equal(2, result[1].Id);
        Assert.Equal("Field 2", result[1].Name);
        Assert.Equal(20.5, result[1].Value);

        Assert.Equal(3, result[2].Id);
        Assert.Equal("Field 3", result[2].Name);
        Assert.Equal(30.5, result[2].Value);
    }

    #endregion

    #region Large Dataset Tests

    [Fact]
    public void ToFrozenArrow_WithLargeStructDataset_HandlesCorrectly()
    {
        // Arrange
        var items = Enumerable.Range(1, 1000).Select(i => new SimpleStruct
        {
            Id = i,
            Name = $"Item {i}",
            Value = i * 1.5
        }).ToArray();

        // Act
        using var collection = items.ToFrozenArrow();

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
    public void ToFrozenArrow_WithLargeReadonlyStructDataset_HandlesCorrectly()
    {
        // Arrange
        var items = Enumerable.Range(1, 1000).Select(i => new ReadonlyStruct
        {
            Id = i,
            Name = $"Readonly {i}",
            Value = i * 2.5
        }).ToArray();

        // Act
        using var collection = items.ToFrozenArrow();

        // Assert
        Assert.Equal(1000, collection.Count);

        var result = collection.ToList();
        Assert.Equal(1000, result.Count);

        // Spot check a few items
        Assert.Equal(1, result[0].Id);
        Assert.Equal("Readonly 1", result[0].Name);
        Assert.Equal(2.5, result[0].Value);

        Assert.Equal(500, result[499].Id);
        Assert.Equal("Readonly 500", result[499].Name);
        Assert.Equal(1250.0, result[499].Value);
    }

    #endregion

    #region Immutability Tests

    [Fact]
    public void ToFrozenArrow_StructIsImmutable_OriginalDataChangesDoNotAffect()
    {
        // Arrange
        var items = new List<SimpleStruct>
        {
            new() { Id = 1, Name = "Item 1", Value = 10.5 },
            new() { Id = 2, Name = "Item 2", Value = 20.5 }
        };

        // Act
        using var collection = items.ToFrozenArrow();

        // Modify original list (note: struct items are copied, but testing list modification)
        items.Add(new SimpleStruct { Id = 3, Name = "Item 3", Value = 30.5 });

        // Assert - collection should contain original data
        var result = collection.ToList();
        Assert.Equal(2, result.Count); // Still 2 items, not 3
    }

    #endregion

    #region Default Value Tests

    [Fact]
    public void ToFrozenArrow_WithDefaultStruct_HandlesDefaults()
    {
        // Arrange - create structs with default values
        var items = new SimpleStruct[]
        {
            default,
            new() { Id = 1, Name = "Item", Value = 10.0 }
        };

        // Act
        using var collection = items.ToFrozenArrow();

        // Assert
        var result = collection.ToList();
        Assert.Equal(2, result.Count);

        // Default struct has default values
        Assert.Equal(0, result[0].Id);
        Assert.Null(result[0].Name); // string default is null
        Assert.Equal(0.0, result[0].Value);

        // Non-default struct
        Assert.Equal(1, result[1].Id);
        Assert.Equal("Item", result[1].Name);
        Assert.Equal(10.0, result[1].Value);
    }

    #endregion

    #region Dispose Tests

    [Fact]
    public void Dispose_WithStruct_PreventsEnumeration()
    {
        // Arrange
        var items = new[]
        {
            new SimpleStruct { Id = 1, Name = "Item 1", Value = 10.5 }
        };

        var collection = items.ToFrozenArrow();

        // Act
        collection.Dispose();

        // Assert
        Assert.Throws<ObjectDisposedException>(() => collection.GetEnumerator());
    }

    [Fact]
    public void Dispose_WithStruct_CanBeCalledMultipleTimes()
    {
        // Arrange
        var items = new[]
        {
            new SimpleStruct { Id = 1, Name = "Item 1", Value = 10.5 }
        };

        var collection = items.ToFrozenArrow();

        // Act & Assert - should not throw
        collection.Dispose();
        collection.Dispose();
        collection.Dispose();
    }

    #endregion
}

/// <summary>
/// Tests for edge cases and special scenarios with structs.
/// </summary>
public class FrozenArrowStructEdgeCaseTests
{
    /// <summary>
    /// Struct with all numeric types to test full type coverage.
    /// </summary>
    [ArrowRecord]
    public struct AllNumericTypesStruct
    {
        [ArrowArray]
        public sbyte SByteValue { get; set; }
        [ArrowArray]
        public short ShortValue { get; set; }
        [ArrowArray]
        public int IntValue { get; set; }
        [ArrowArray]
        public long LongValue { get; set; }
        [ArrowArray]
        public byte ByteValue { get; set; }
        [ArrowArray]
        public ushort UShortValue { get; set; }
        [ArrowArray]
        public uint UIntValue { get; set; }
        [ArrowArray]
        public ulong ULongValue { get; set; }
        [ArrowArray]
        public float FloatValue { get; set; }
        [ArrowArray]
        public double DoubleValue { get; set; }
    }

    [Fact]
    public void ToFrozenArrow_WithAllNumericTypes_PreservesValues()
    {
        // Arrange
        var items = new[]
        {
            new AllNumericTypesStruct
            {
                SByteValue = -128,
                ShortValue = -32768,
                IntValue = -2147483648,
                LongValue = -9223372036854775808L,
                ByteValue = 255,
                UShortValue = 65535,
                UIntValue = 4294967295U,
                ULongValue = 18446744073709551615UL,
                FloatValue = float.MaxValue,
                DoubleValue = double.MaxValue
            },
            new AllNumericTypesStruct
            {
                SByteValue = 127,
                ShortValue = 32767,
                IntValue = 2147483647,
                LongValue = 9223372036854775807L,
                ByteValue = 0,
                UShortValue = 0,
                UIntValue = 0,
                ULongValue = 0,
                FloatValue = float.MinValue,
                DoubleValue = double.MinValue
            }
        };

        // Act
        using var collection = items.ToFrozenArrow();

        // Assert
        var result = collection.ToList();
        Assert.Equal(2, result.Count);

        // Check max/min values
        Assert.Equal((sbyte)-128, result[0].SByteValue);
        Assert.Equal((short)-32768, result[0].ShortValue);
        Assert.Equal(-2147483648, result[0].IntValue);
        Assert.Equal(-9223372036854775808L, result[0].LongValue);
        Assert.Equal((byte)255, result[0].ByteValue);
        Assert.Equal((ushort)65535, result[0].UShortValue);
        Assert.Equal(4294967295U, result[0].UIntValue);
        Assert.Equal(18446744073709551615UL, result[0].ULongValue);
        Assert.Equal(float.MaxValue, result[0].FloatValue);
        Assert.Equal(double.MaxValue, result[0].DoubleValue);

        Assert.Equal((sbyte)127, result[1].SByteValue);
        Assert.Equal((short)32767, result[1].ShortValue);
        Assert.Equal(2147483647, result[1].IntValue);
        Assert.Equal(9223372036854775807L, result[1].LongValue);
        Assert.Equal((byte)0, result[1].ByteValue);
        Assert.Equal((ushort)0, result[1].UShortValue);
        Assert.Equal(0U, result[1].UIntValue);
        Assert.Equal(0UL, result[1].ULongValue);
        Assert.Equal(float.MinValue, result[1].FloatValue);
        Assert.Equal(double.MinValue, result[1].DoubleValue);
    }

    /// <summary>
    /// Struct with only a single field.
    /// </summary>
    [ArrowRecord]
    public struct SingleFieldStruct
    {
        [ArrowArray]
        public int Id { get; set; }
    }

    [Fact]
    public void ToFrozenArrow_WithSingleFieldStruct_Works()
    {
        // Arrange
        var items = new[]
        {
            new SingleFieldStruct { Id = 1 },
            new SingleFieldStruct { Id = 2 },
            new SingleFieldStruct { Id = 3 }
        };

        // Act
        using var collection = items.ToFrozenArrow();

        // Assert
        var result = collection.ToList();
        Assert.Equal(3, result.Count);
        Assert.Equal(1, result[0].Id);
        Assert.Equal(2, result[1].Id);
        Assert.Equal(3, result[2].Id);
    }

    /// <summary>
    /// Readonly struct with only a single field.
    /// </summary>
    [ArrowRecord]
    public readonly struct SingleFieldReadonlyStruct
    {
        [ArrowArray]
        public int Id { get; init; }
    }

    [Fact]
    public void ToFrozenArrow_WithSingleFieldReadonlyStruct_Works()
    {
        // Arrange
        var items = new[]
        {
            new SingleFieldReadonlyStruct { Id = 10 },
            new SingleFieldReadonlyStruct { Id = 20 },
            new SingleFieldReadonlyStruct { Id = 30 }
        };

        // Act
        using var collection = items.ToFrozenArrow();

        // Assert
        var result = collection.ToList();
        Assert.Equal(3, result.Count);
        Assert.Equal(10, result[0].Id);
        Assert.Equal(20, result[1].Id);
        Assert.Equal(30, result[2].Id);
    }

    [Fact]
    public void ToFrozenArrow_WithLinqQueryOnStruct_Works()
    {
        // Arrange
        var items = Enumerable.Range(1, 100).Select(i => new SingleFieldStruct { Id = i });

        // Act
        using var collection = items.ToFrozenArrow();

        // Assert
        Assert.Equal(100, collection.Count);
        
        // LINQ operations work on the collection
        var sum = collection.Sum(x => x.Id);
        Assert.Equal(5050, sum); // Sum of 1 to 100

        var filtered = collection.Where(x => x.Id > 50).ToList();
        Assert.Equal(50, filtered.Count);
        Assert.Equal(51, filtered[0].Id);
    }

    /// <summary>
    /// Struct with boolean fields for testing.
    /// </summary>
    [ArrowRecord]
    public struct BoolStruct
    {
        [ArrowArray]
        public bool Value1 { get; set; }
        [ArrowArray]
        public bool Value2 { get; set; }
        [ArrowArray]
        public bool Value3 { get; set; }
    }

    [Fact]
    public void ToFrozenArrow_WithStructContainingBooleans_PreservesBoolValues()
    {
        // Arrange - test various boolean patterns
        var items = new[]
        {
            new BoolStruct { Value1 = true, Value2 = false, Value3 = true },
            new BoolStruct { Value1 = false, Value2 = true, Value3 = false },
            new BoolStruct { Value1 = true, Value2 = true, Value3 = true },
            new BoolStruct { Value1 = false, Value2 = false, Value3 = false }
        };

        // Act
        using var collection = items.ToFrozenArrow();

        // Assert
        var result = collection.ToList();
        Assert.Equal(4, result.Count);

        Assert.True(result[0].Value1);
        Assert.False(result[0].Value2);
        Assert.True(result[0].Value3);

        Assert.False(result[1].Value1);
        Assert.True(result[1].Value2);
        Assert.False(result[1].Value3);

        Assert.True(result[2].Value1);
        Assert.True(result[2].Value2);
        Assert.True(result[2].Value3);

        Assert.False(result[3].Value1);
        Assert.False(result[3].Value2);
        Assert.False(result[3].Value3);
    }
}


