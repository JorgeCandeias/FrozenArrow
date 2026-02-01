using System.Buffers;

namespace ArrowCollection.Tests;

/// <summary>
/// Tests for positional records (record classes and record structs with primary constructors).
/// </summary>
public class PositionalRecordTests
{
    #region Test Models - Positional Record Classes

    /// <summary>
    /// Simple positional record class - no parameterless constructor.
    /// </summary>
    [ArrowRecord]
    public record PersonRecord(
        [property: ArrowArray] int Id,
        [property: ArrowArray] string Name,
        [property: ArrowArray] double Salary);

    /// <summary>
    /// Positional record with explicit column names.
    /// </summary>
    [ArrowRecord]
    public record NamedPersonRecord(
        [property: ArrowArray(Name = "person_id")] int Id,
        [property: ArrowArray(Name = "person_name")] string Name,
        [property: ArrowArray(Name = "hire_date")] DateTime HireDate);

    /// <summary>
    /// Positional record with nullable properties.
    /// </summary>
    [ArrowRecord]
    public record NullablePersonRecord(
        [property: ArrowArray] int Id,
        [property: ArrowArray] string? Name,
        [property: ArrowArray] int? Age);

    /// <summary>
    /// Positional record with mixed positional and additional properties.
    /// </summary>
    [ArrowRecord]
    public record MixedRecord(
        [property: ArrowArray] int Id,
        [property: ArrowArray] string Name)
    {
        [ArrowArray]
        public double Value { get; init; }

        [ArrowArray]
        public DateTime CreatedAt { get; init; }
    }

    #endregion

    #region Test Models - Positional Record Structs

    /// <summary>
    /// Simple positional record struct.
    /// </summary>
    [ArrowRecord]
    public record struct PointRecord(
        [property: ArrowArray] int X,
        [property: ArrowArray] int Y,
        [property: ArrowArray] int Z);

    /// <summary>
    /// Readonly positional record struct.
    /// </summary>
    [ArrowRecord]
    public readonly record struct ReadonlyPointRecord(
        [property: ArrowArray] double X,
        [property: ArrowArray] double Y);

    /// <summary>
    /// Record struct with explicit names.
    /// </summary>
    [ArrowRecord]
    public record struct NamedPointRecord(
        [property: ArrowArray(Name = "x_coord")] int X,
        [property: ArrowArray(Name = "y_coord")] int Y);

    #endregion

    #region Positional Record Class Tests

    [Fact]
    public void ToArrowCollection_WithPositionalRecord_Works()
    {
        // Arrange
        var items = new[]
        {
            new PersonRecord(1, "Alice", 50000.0),
            new PersonRecord(2, "Bob", 60000.0),
            new PersonRecord(3, "Charlie", 70000.0)
        };

        // Act
        using var collection = items.ToArrowCollection();

        // Assert
        Assert.Equal(3, collection.Count);

        var result = collection.ToList();
        Assert.Equal(1, result[0].Id);
        Assert.Equal("Alice", result[0].Name);
        Assert.Equal(50000.0, result[0].Salary);

        Assert.Equal(2, result[1].Id);
        Assert.Equal("Bob", result[1].Name);
        Assert.Equal(60000.0, result[1].Salary);
    }

    [Fact]
    public void ToArrowCollection_WithPositionalRecordAndExplicitNames_Works()
    {
        // Arrange
        var hireDate = new DateTime(2023, 1, 15, 0, 0, 0, DateTimeKind.Utc);
        var items = new[]
        {
            new NamedPersonRecord(1, "Alice", hireDate),
            new NamedPersonRecord(2, "Bob", hireDate.AddDays(30))
        };

        // Act
        using var collection = items.ToArrowCollection();

        // Assert
        Assert.Equal(2, collection.Count);

        var result = collection.ToList();
        Assert.Equal(1, result[0].Id);
        Assert.Equal("Alice", result[0].Name);
    }

    [Fact]
    public void ToArrowCollection_WithPositionalRecordAndNullables_PreservesNulls()
    {
        // Arrange
        var items = new[]
        {
            new NullablePersonRecord(1, "Alice", 30),
            new NullablePersonRecord(2, null, null),
            new NullablePersonRecord(3, "Charlie", null)
        };

        // Act
        using var collection = items.ToArrowCollection();

        // Assert
        var result = collection.ToList();

        Assert.Equal("Alice", result[0].Name);
        Assert.Equal(30, result[0].Age);

        Assert.Null(result[1].Name);
        Assert.Null(result[1].Age);

        Assert.Equal("Charlie", result[2].Name);
        Assert.Null(result[2].Age);
    }

    [Fact]
    public void ToArrowCollection_WithMixedPositionalAndAdditionalProperties_Works()
    {
        // Arrange
        var createdAt = new DateTime(2023, 6, 1, 0, 0, 0, DateTimeKind.Utc);
        var items = new[]
        {
            new MixedRecord(1, "Item1") { Value = 100.0, CreatedAt = createdAt },
            new MixedRecord(2, "Item2") { Value = 200.0, CreatedAt = createdAt.AddDays(1) }
        };

        // Act
        using var collection = items.ToArrowCollection();

        // Assert
        var result = collection.ToList();
        Assert.Equal(2, result.Count);

        Assert.Equal(1, result[0].Id);
        Assert.Equal("Item1", result[0].Name);
        Assert.Equal(100.0, result[0].Value);

        Assert.Equal(2, result[1].Id);
        Assert.Equal("Item2", result[1].Name);
        Assert.Equal(200.0, result[1].Value);
    }

    #endregion

    #region Positional Record Struct Tests

    [Fact]
    public void ToArrowCollection_WithPositionalRecordStruct_Works()
    {
        // Arrange
        var items = new[]
        {
            new PointRecord(1, 2, 3),
            new PointRecord(4, 5, 6),
            new PointRecord(7, 8, 9)
        };

        // Act
        using var collection = items.ToArrowCollection();

        // Assert
        Assert.Equal(3, collection.Count);

        var result = collection.ToList();
        Assert.Equal(1, result[0].X);
        Assert.Equal(2, result[0].Y);
        Assert.Equal(3, result[0].Z);

        Assert.Equal(7, result[2].X);
        Assert.Equal(8, result[2].Y);
        Assert.Equal(9, result[2].Z);
    }

    [Fact]
    public void ToArrowCollection_WithReadonlyRecordStruct_Works()
    {
        // Arrange
        var items = new[]
        {
            new ReadonlyPointRecord(1.5, 2.5),
            new ReadonlyPointRecord(3.5, 4.5)
        };

        // Act
        using var collection = items.ToArrowCollection();

        // Assert
        var result = collection.ToList();
        Assert.Equal(2, result.Count);

        Assert.Equal(1.5, result[0].X);
        Assert.Equal(2.5, result[0].Y);

        Assert.Equal(3.5, result[1].X);
        Assert.Equal(4.5, result[1].Y);
    }

    [Fact]
    public void ToArrowCollection_WithNamedRecordStruct_Works()
    {
        // Arrange
        var items = new[]
        {
            new NamedPointRecord(10, 20),
            new NamedPointRecord(30, 40)
        };

        // Act
        using var collection = items.ToArrowCollection();

        // Assert
        var result = collection.ToList();
        Assert.Equal(10, result[0].X);
        Assert.Equal(20, result[0].Y);
    }

    #endregion

    #region Serialization Round-Trip Tests

    [Fact]
    public void RoundTrip_WithPositionalRecord_PreservesData()
    {
        // Arrange
        var items = new[]
        {
            new PersonRecord(1, "Alice", 50000.0),
            new PersonRecord(2, "Bob", 60000.0)
        };

        using var original = items.ToArrowCollection();

        // Act - Serialize
        var buffer = new ArrayBufferWriter<byte>();
        original.WriteTo(buffer);

        // Act - Deserialize
        using var deserialized = ArrowCollection<PersonRecord>.ReadFrom(buffer.WrittenSpan);

        // Assert
        Assert.Equal(original.Count, deserialized.Count);

        var result = deserialized.ToList();
        Assert.Equal(1, result[0].Id);
        Assert.Equal("Alice", result[0].Name);
        Assert.Equal(50000.0, result[0].Salary);

        Assert.Equal(2, result[1].Id);
        Assert.Equal("Bob", result[1].Name);
        Assert.Equal(60000.0, result[1].Salary);
    }

    [Fact]
    public void RoundTrip_WithPositionalRecordStruct_PreservesData()
    {
        // Arrange
        var items = new[]
        {
            new PointRecord(1, 2, 3),
            new PointRecord(4, 5, 6)
        };

        using var original = items.ToArrowCollection();

        // Act
        var buffer = new ArrayBufferWriter<byte>();
        original.WriteTo(buffer);
        using var deserialized = ArrowCollection<PointRecord>.ReadFrom(buffer.WrittenSpan);

        // Assert
        var result = deserialized.ToList();
        Assert.Equal(1, result[0].X);
        Assert.Equal(2, result[0].Y);
        Assert.Equal(3, result[0].Z);
    }

    [Fact]
    public void RoundTrip_WithReadonlyRecordStruct_PreservesData()
    {
        // Arrange
        var items = new[]
        {
            new ReadonlyPointRecord(1.5, 2.5),
            new ReadonlyPointRecord(3.5, 4.5)
        };

        using var original = items.ToArrowCollection();

        // Act
        var buffer = new ArrayBufferWriter<byte>();
        original.WriteTo(buffer);
        using var deserialized = ArrowCollection<ReadonlyPointRecord>.ReadFrom(buffer.WrittenSpan);

        // Assert
        var result = deserialized.ToList();
        Assert.Equal(1.5, result[0].X);
        Assert.Equal(2.5, result[0].Y);
    }

    [Fact]
    public async Task RoundTripAsync_WithPositionalRecord_PreservesData()
    {
        // Arrange
        var items = new[]
        {
            new PersonRecord(1, "Alice", 50000.0),
            new PersonRecord(2, "Bob", 60000.0)
        };

        using var original = items.ToArrowCollection();
        using var stream = new MemoryStream();

        // Act
        await original.WriteToAsync(stream);
        stream.Position = 0;
        using var deserialized = await ArrowCollection<PersonRecord>.ReadFromAsync(stream);

        // Assert
        var result = deserialized.ToList();
        Assert.Equal(2, result.Count);
        Assert.Equal("Alice", result[0].Name);
        Assert.Equal("Bob", result[1].Name);
    }

    #endregion

    #region Large Dataset Tests

    [Fact]
    public void ToArrowCollection_WithLargePositionalRecordDataset_Works()
    {
        // Arrange
        var items = Enumerable.Range(1, 10000)
            .Select(i => new PersonRecord(i, $"Person_{i}", i * 1000.0))
            .ToArray();

        // Act
        using var collection = items.ToArrowCollection();

        // Assert
        Assert.Equal(10000, collection.Count);

        var result = collection.ToList();
        Assert.Equal(1, result[0].Id);
        Assert.Equal("Person_1", result[0].Name);
        Assert.Equal(1000.0, result[0].Salary);

        Assert.Equal(10000, result[9999].Id);
        Assert.Equal("Person_10000", result[9999].Name);
        Assert.Equal(10000000.0, result[9999].Salary);
    }

    [Fact]
    public void ToArrowCollection_WithLargeRecordStructDataset_Works()
    {
        // Arrange
        var items = Enumerable.Range(1, 10000)
            .Select(i => new PointRecord(i, i * 2, i * 3))
            .ToArray();

        // Act
        using var collection = items.ToArrowCollection();

        // Assert
        Assert.Equal(10000, collection.Count);

        var result = collection.ToList();
        Assert.Equal(1, result[0].X);
        Assert.Equal(10000, result[9999].X);
        Assert.Equal(20000, result[9999].Y);
        Assert.Equal(30000, result[9999].Z);
    }

    #endregion
}
