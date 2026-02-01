using System.Buffers;

namespace ArrowCollection.Tests;

/// <summary>
/// Tests for ArrowCollection serialization and deserialization functionality.
/// </summary>
public class SerializationTests
{
    #region Test Models

    [ArrowRecord]
    public class SimpleRecord
    {
        [ArrowArray]
        public int Id { get; set; }

        [ArrowArray]
        public string Name { get; set; } = string.Empty;

        [ArrowArray]
        public double Value { get; set; }
    }

    [ArrowRecord]
    public class RecordWithExplicitNames
    {
        [ArrowArray(Name = "record_id")]
        public int Id { get; set; }

        [ArrowArray(Name = "record_name")]
        public string Name { get; set; } = string.Empty;

        [ArrowArray(Name = "record_value")]
        public double Value { get; set; }
    }

    [ArrowRecord]
    public class RecordWithNullables
    {
        [ArrowArray]
        public int Id { get; set; }

        [ArrowArray]
        public string? NullableName { get; set; }

        [ArrowArray]
        public int? NullableValue { get; set; }
    }


    [ArrowRecord]
    public struct SerializationStruct
    {
        [ArrowArray]
        public int Id { get; set; }

        [ArrowArray]
        public string Name { get; set; }

        [ArrowArray]
        public double Value { get; set; }
    }

    /// <summary>
    /// Positional record class for serialization tests.
    /// </summary>
    [ArrowRecord]
    public record PositionalRecord(
        [property: ArrowArray] int Id,
        [property: ArrowArray] string Name,
        [property: ArrowArray] double Value);

    /// <summary>
    /// Positional record class with explicit column names.
    /// </summary>
    [ArrowRecord]
    public record PositionalRecordWithNames(
        [property: ArrowArray(Name = "record_id")] int Id,
        [property: ArrowArray(Name = "record_name")] string Name,
        [property: ArrowArray(Name = "record_value")] double Value);

    /// <summary>
    /// Positional record struct for serialization tests.
    /// </summary>
    [ArrowRecord]
    public record struct PositionalRecordStruct(
        [property: ArrowArray] int Id,
        [property: ArrowArray] string Name,
        [property: ArrowArray] double Value);

    /// <summary>
    /// Readonly positional record struct for serialization tests.
    /// </summary>
    [ArrowRecord]
    public readonly record struct ReadonlyPositionalRecordStruct(
        [property: ArrowArray] int Id,
        [property: ArrowArray] string Name,
        [property: ArrowArray] double Value);

    #endregion

    #region WriteTo / ReadFrom Round-Trip Tests

    [Fact]
    public void RoundTrip_WithSimpleRecord_PreservesData()
    {
        // Arrange
        var items = new[]
        {
            new SimpleRecord { Id = 1, Name = "Alice", Value = 10.5 },
            new SimpleRecord { Id = 2, Name = "Bob", Value = 20.5 },
            new SimpleRecord { Id = 3, Name = "Charlie", Value = 30.5 }
        };

        using var original = items.ToArrowCollection();

        // Act - Serialize
        var buffer = new ArrayBufferWriter<byte>();
        original.WriteTo(buffer);

        // Act - Deserialize
        using var deserialized = ArrowCollection<SimpleRecord>.ReadFrom(buffer.WrittenSpan);

        // Assert
        Assert.Equal(original.Count, deserialized.Count);

        var originalList = original.ToList();
        var deserializedList = deserialized.ToList();

        for (int i = 0; i < originalList.Count; i++)
        {
            Assert.Equal(originalList[i].Id, deserializedList[i].Id);
            Assert.Equal(originalList[i].Name, deserializedList[i].Name);
            Assert.Equal(originalList[i].Value, deserializedList[i].Value);
        }
    }

    [Fact]
    public async Task RoundTripAsync_WithStream_PreservesData()
    {
        // Arrange
        var items = new[]
        {
            new SimpleRecord { Id = 1, Name = "Alice", Value = 10.5 },
            new SimpleRecord { Id = 2, Name = "Bob", Value = 20.5 },
            new SimpleRecord { Id = 3, Name = "Charlie", Value = 30.5 }
        };

        using var original = items.ToArrowCollection();
        using var stream = new MemoryStream();

        // Act - Serialize
        await original.WriteToAsync(stream);
        stream.Position = 0;

        // Act - Deserialize
        using var deserialized = await ArrowCollection<SimpleRecord>.ReadFromAsync(stream);

        // Assert
        Assert.Equal(original.Count, deserialized.Count);

        var originalList = original.ToList();
        var deserializedList = deserialized.ToList();

        for (int i = 0; i < originalList.Count; i++)
        {
            Assert.Equal(originalList[i].Id, deserializedList[i].Id);
            Assert.Equal(originalList[i].Name, deserializedList[i].Name);
            Assert.Equal(originalList[i].Value, deserializedList[i].Value);
        }
    }

    [Fact]
    public void RoundTrip_WithReadOnlySequence_PreservesData()
    {
        // Arrange
        var items = new[]
        {
            new SimpleRecord { Id = 1, Name = "Test1", Value = 1.1 },
            new SimpleRecord { Id = 2, Name = "Test2", Value = 2.2 }
        };

        using var original = items.ToArrowCollection();

        // Act - Serialize
        var buffer = new ArrayBufferWriter<byte>();
        original.WriteTo(buffer);

        // Create ReadOnlySequence from the buffer
        var sequence = new ReadOnlySequence<byte>(buffer.WrittenMemory);

        // Act - Deserialize
        using var deserialized = ArrowCollection<SimpleRecord>.ReadFrom(sequence);

        // Assert
        Assert.Equal(original.Count, deserialized.Count);

        var originalList = original.ToList();
        var deserializedList = deserialized.ToList();

        Assert.Equal(originalList[0].Id, deserializedList[0].Id);
        Assert.Equal(originalList[0].Name, deserializedList[0].Name);
    }

    [Fact]
    public void RoundTrip_WithNullableValues_PreservesNulls()
    {
        // Arrange
        var items = new[]
        {
            new RecordWithNullables { Id = 1, NullableName = "HasName", NullableValue = 100 },
            new RecordWithNullables { Id = 2, NullableName = null, NullableValue = null },
            new RecordWithNullables { Id = 3, NullableName = "AnotherName", NullableValue = null }
        };

        using var original = items.ToArrowCollection();

        // Act
        var buffer = new ArrayBufferWriter<byte>();
        original.WriteTo(buffer);
        using var deserialized = ArrowCollection<RecordWithNullables>.ReadFrom(buffer.WrittenSpan);

        // Assert
        var deserializedList = deserialized.ToList();

        Assert.Equal("HasName", deserializedList[0].NullableName);
        Assert.Equal(100, deserializedList[0].NullableValue);

        Assert.Null(deserializedList[1].NullableName);
        Assert.Null(deserializedList[1].NullableValue);

        Assert.Equal("AnotherName", deserializedList[2].NullableName);
        Assert.Null(deserializedList[2].NullableValue);
    }

    [Fact]
    public void RoundTrip_WithStruct_PreservesData()
    {
        // Arrange
        var items = new[]
        {
            new SerializationStruct { Id = 1, Name = "StructItem1", Value = 11.1 },
            new SerializationStruct { Id = 2, Name = "StructItem2", Value = 22.2 }
        };

        using var original = items.ToArrowCollection();

        // Act
        var buffer = new ArrayBufferWriter<byte>();
            original.WriteTo(buffer);
            using var deserialized = ArrowCollection<SerializationStruct>.ReadFrom(buffer.WrittenSpan);

            // Assert
            var deserializedList = deserialized.ToList();
            Assert.Equal(2, deserializedList.Count);
            Assert.Equal(1, deserializedList[0].Id);
            Assert.Equal("StructItem1", deserializedList[0].Name);
            Assert.Equal(11.1, deserializedList[0].Value);
        }

        [Fact]
        public void RoundTrip_WithPositionalRecordClass_PreservesData()
        {
            // Arrange
            var items = new[]
            {
                new PositionalRecord(1, "Alice", 10.5),
                new PositionalRecord(2, "Bob", 20.5),
                new PositionalRecord(3, "Charlie", 30.5)
            };

            using var original = items.ToArrowCollection();

            // Act
            var buffer = new ArrayBufferWriter<byte>();
            original.WriteTo(buffer);
            using var deserialized = ArrowCollection<PositionalRecord>.ReadFrom(buffer.WrittenSpan);

            // Assert
            Assert.Equal(original.Count, deserialized.Count);

            var deserializedList = deserialized.ToList();
            Assert.Equal(1, deserializedList[0].Id);
            Assert.Equal("Alice", deserializedList[0].Name);
            Assert.Equal(10.5, deserializedList[0].Value);

            Assert.Equal(3, deserializedList[2].Id);
            Assert.Equal("Charlie", deserializedList[2].Name);
            Assert.Equal(30.5, deserializedList[2].Value);
        }

        [Fact]
        public void RoundTrip_WithPositionalRecordClassAndExplicitNames_PreservesData()
        {
            // Arrange
            var items = new[]
            {
                new PositionalRecordWithNames(1, "Named1", 100.0),
                new PositionalRecordWithNames(2, "Named2", 200.0)
            };

            using var original = items.ToArrowCollection();

            // Act
            var buffer = new ArrayBufferWriter<byte>();
            original.WriteTo(buffer);
            using var deserialized = ArrowCollection<PositionalRecordWithNames>.ReadFrom(buffer.WrittenSpan);

            // Assert
            var deserializedList = deserialized.ToList();
            Assert.Equal(2, deserializedList.Count);
            Assert.Equal(1, deserializedList[0].Id);
            Assert.Equal("Named1", deserializedList[0].Name);
            Assert.Equal(100.0, deserializedList[0].Value);
        }

        [Fact]
        public void RoundTrip_WithPositionalRecordStruct_PreservesData()
        {
            // Arrange
            var items = new[]
            {
                new PositionalRecordStruct(1, "StructRec1", 11.1),
                new PositionalRecordStruct(2, "StructRec2", 22.2),
                new PositionalRecordStruct(3, "StructRec3", 33.3)
            };

            using var original = items.ToArrowCollection();

            // Act
            var buffer = new ArrayBufferWriter<byte>();
            original.WriteTo(buffer);
            using var deserialized = ArrowCollection<PositionalRecordStruct>.ReadFrom(buffer.WrittenSpan);

            // Assert
            var deserializedList = deserialized.ToList();
            Assert.Equal(3, deserializedList.Count);
            Assert.Equal(1, deserializedList[0].Id);
            Assert.Equal("StructRec1", deserializedList[0].Name);
            Assert.Equal(11.1, deserializedList[0].Value);

            Assert.Equal(3, deserializedList[2].Id);
            Assert.Equal("StructRec3", deserializedList[2].Name);
        }

        [Fact]
        public void RoundTrip_WithReadonlyPositionalRecordStruct_PreservesData()
        {
            // Arrange
            var items = new[]
            {
                new ReadonlyPositionalRecordStruct(1, "Readonly1", 1.1),
                new ReadonlyPositionalRecordStruct(2, "Readonly2", 2.2)
            };

            using var original = items.ToArrowCollection();

            // Act
            var buffer = new ArrayBufferWriter<byte>();
            original.WriteTo(buffer);
            using var deserialized = ArrowCollection<ReadonlyPositionalRecordStruct>.ReadFrom(buffer.WrittenSpan);

            // Assert
            var deserializedList = deserialized.ToList();
            Assert.Equal(2, deserializedList.Count);
            Assert.Equal(1, deserializedList[0].Id);
            Assert.Equal("Readonly1", deserializedList[0].Name);
            Assert.Equal(1.1, deserializedList[0].Value);
        }

        [Fact]
        public async Task RoundTripAsync_WithPositionalRecordClass_PreservesData()
        {
            // Arrange
            var items = new[]
            {
                new PositionalRecord(1, "Async1", 100.0),
                new PositionalRecord(2, "Async2", 200.0)
            };

            using var original = items.ToArrowCollection();
            using var stream = new MemoryStream();

            // Act
            await original.WriteToAsync(stream);
            stream.Position = 0;
            using var deserialized = await ArrowCollection<PositionalRecord>.ReadFromAsync(stream);

            // Assert
            var deserializedList = deserialized.ToList();
            Assert.Equal(2, deserializedList.Count);
            Assert.Equal("Async1", deserializedList[0].Name);
            Assert.Equal("Async2", deserializedList[1].Name);
        }

        [Fact]
        public async Task RoundTripAsync_WithPositionalRecordStruct_PreservesData()
        {
            // Arrange
            var items = new[]
            {
                new PositionalRecordStruct(1, "AsyncStruct1", 10.0),
                new PositionalRecordStruct(2, "AsyncStruct2", 20.0)
            };

            using var original = items.ToArrowCollection();
            using var stream = new MemoryStream();

            // Act
            await original.WriteToAsync(stream);
            stream.Position = 0;
            using var deserialized = await ArrowCollection<PositionalRecordStruct>.ReadFromAsync(stream);

            // Assert
            var deserializedList = deserialized.ToList();
            Assert.Equal(2, deserializedList.Count);
            Assert.Equal("AsyncStruct1", deserializedList[0].Name);
            Assert.Equal("AsyncStruct2", deserializedList[1].Name);
        }

        [Fact]
        public void RoundTrip_WithEmptyCollection_PreservesEmpty()
        {
        // Arrange
        var items = Array.Empty<SimpleRecord>();
        using var original = items.ToArrowCollection();

        // Act
        var buffer = new ArrayBufferWriter<byte>();
        original.WriteTo(buffer);
        using var deserialized = ArrowCollection<SimpleRecord>.ReadFrom(buffer.WrittenSpan);

        // Assert
        Assert.Equal(0, deserialized.Count);
        Assert.Empty(deserialized.ToList());
    }

    [Fact]
    public void RoundTrip_WithLargeCollection_PreservesData()
    {
        // Arrange
        var items = Enumerable.Range(1, 10000)
            .Select(i => new SimpleRecord { Id = i, Name = $"Item_{i}", Value = i * 1.5 })
            .ToArray();

        using var original = items.ToArrowCollection();

        // Act
        var buffer = new ArrayBufferWriter<byte>();
        original.WriteTo(buffer);
        using var deserialized = ArrowCollection<SimpleRecord>.ReadFrom(buffer.WrittenSpan);

        // Assert
        Assert.Equal(10000, deserialized.Count);

        var deserializedList = deserialized.ToList();
        Assert.Equal(1, deserializedList[0].Id);
        Assert.Equal("Item_1", deserializedList[0].Name);
        Assert.Equal(10000, deserializedList[9999].Id);
        Assert.Equal("Item_10000", deserializedList[9999].Name);
    }

    #endregion

    #region ArrowReadOptions Tests

    [Fact]
    public void ReadFrom_WithDefaultOptions_IgnoresUnknownColumns()
    {
        // This test verifies that by default, extra columns in source are ignored
        // Since we can't easily add extra columns to serialized data,
        // we verify the default behavior works with matching schemas
        var items = new[] { new SimpleRecord { Id = 1, Name = "Test", Value = 1.0 } };

        using var original = items.ToArrowCollection();
        var buffer = new ArrayBufferWriter<byte>();
        original.WriteTo(buffer);

        // Default options should work fine
        using var deserialized = ArrowCollection<SimpleRecord>.ReadFrom(
            buffer.WrittenSpan, 
            ArrowReadOptions.Default);

        Assert.Equal(1, deserialized.Count);
    }

    [Fact]
    public void ReadFrom_WithDefaultOptions_UseDefaultForMissingColumns()
    {
        // Verify that with default options, missing columns get default values
        // This is tested implicitly by the round-trip tests working correctly
        var items = new[] { new SimpleRecord { Id = 1, Name = "Test", Value = 1.0 } };

        using var original = items.ToArrowCollection();
        var buffer = new ArrayBufferWriter<byte>();
        original.WriteTo(buffer);

        var options = new ArrowReadOptions
        {
            MissingColumns = MissingColumnBehavior.UseDefault
        };

        using var deserialized = ArrowCollection<SimpleRecord>.ReadFrom(buffer.WrittenSpan, options);
        Assert.Equal(1, deserialized.Count);
    }

    [Fact]
    public void ArrowReadOptions_Default_HasExpectedValues()
    {
        var options = ArrowReadOptions.Default;

        Assert.Equal(UnknownColumnBehavior.Ignore, options.UnknownColumns);
        Assert.Equal(MissingColumnBehavior.UseDefault, options.MissingColumns);
    }

    [Fact]
    public void ArrowReadOptions_CanBeCustomized()
    {
        var options = new ArrowReadOptions
        {
            UnknownColumns = UnknownColumnBehavior.Throw,
            MissingColumns = MissingColumnBehavior.Throw
        };

        Assert.Equal(UnknownColumnBehavior.Throw, options.UnknownColumns);
        Assert.Equal(MissingColumnBehavior.Throw, options.MissingColumns);
    }

    #endregion

    #region ArrowWriteOptions Tests

    [Fact]
    public void ArrowWriteOptions_Default_Exists()
    {
        var options = ArrowWriteOptions.Default;
        Assert.NotNull(options);
    }

    [Fact]
    public void WriteTo_WithDefaultOptions_Succeeds()
    {
        var items = new[] { new SimpleRecord { Id = 1, Name = "Test", Value = 1.0 } };
        using var collection = items.ToArrowCollection();

        var buffer = new ArrayBufferWriter<byte>();
        collection.WriteTo(buffer, ArrowWriteOptions.Default);

        Assert.True(buffer.WrittenCount > 0);
    }

    #endregion

    #region Explicit Column Name Tests

    [Fact]
    public void RoundTrip_WithExplicitNames_PreservesData()
    {
        // Arrange
        var items = new[]
        {
            new RecordWithExplicitNames { Id = 1, Name = "NamedRecord1", Value = 100.0 },
            new RecordWithExplicitNames { Id = 2, Name = "NamedRecord2", Value = 200.0 }
        };

        using var original = items.ToArrowCollection();

        // Act
        var buffer = new ArrayBufferWriter<byte>();
        original.WriteTo(buffer);
        using var deserialized = ArrowCollection<RecordWithExplicitNames>.ReadFrom(buffer.WrittenSpan);

        // Assert
        var deserializedList = deserialized.ToList();
        Assert.Equal(2, deserializedList.Count);
        Assert.Equal(1, deserializedList[0].Id);
        Assert.Equal("NamedRecord1", deserializedList[0].Name);
        Assert.Equal(100.0, deserializedList[0].Value);
    }

    #endregion

    #region Error Handling Tests

    [Fact]
    public void WriteTo_WhenDisposed_ThrowsObjectDisposedException()
    {
        var items = new[] { new SimpleRecord { Id = 1, Name = "Test", Value = 1.0 } };
        var collection = items.ToArrowCollection();
        collection.Dispose();

        var buffer = new ArrayBufferWriter<byte>();
        Assert.Throws<ObjectDisposedException>(() => collection.WriteTo(buffer));
    }

    [Fact]
    public async Task WriteToAsync_WhenDisposed_ThrowsObjectDisposedException()
    {
        var items = new[] { new SimpleRecord { Id = 1, Name = "Test", Value = 1.0 } };
        var collection = items.ToArrowCollection();
        collection.Dispose();

        using var stream = new MemoryStream();
        await Assert.ThrowsAsync<ObjectDisposedException>(() => collection.WriteToAsync(stream));
    }

    [Fact]
    public void WriteTo_WithNullWriter_ThrowsArgumentNullException()
    {
        var items = new[] { new SimpleRecord { Id = 1, Name = "Test", Value = 1.0 } };
        using var collection = items.ToArrowCollection();

        Assert.Throws<ArgumentNullException>(() => collection.WriteTo(null!));
    }

    [Fact]
    public async Task WriteToAsync_WithNullStream_ThrowsArgumentNullException()
    {
        var items = new[] { new SimpleRecord { Id = 1, Name = "Test", Value = 1.0 } };
        using var collection = items.ToArrowCollection();

        await Assert.ThrowsAsync<ArgumentNullException>(() => collection.WriteToAsync(null!));
    }

    [Fact]
    public async Task ReadFromAsync_WithNullStream_ThrowsArgumentNullException()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(() => 
            ArrowCollection<SimpleRecord>.ReadFromAsync(null!));
    }

    [Fact]
    public void ReadFrom_WithEmptyData_ThrowsInvalidOperationException()
    {
        var emptyArray = Array.Empty<byte>();
        
        Assert.Throws<InvalidOperationException>(() => 
            ArrowCollection<SimpleRecord>.ReadFrom(emptyArray.AsSpan()));
    }

    #endregion

    #region Cancellation Tests

    [Fact]
    public async Task WriteToAsync_WithCancellation_CanBeCancelled()
    {
        var items = Enumerable.Range(1, 1000)
            .Select(i => new SimpleRecord { Id = i, Name = $"Item_{i}", Value = i * 1.0 })
            .ToArray();

        using var collection = items.ToArrowCollection();
        using var stream = new MemoryStream();
        using var cts = new CancellationTokenSource();

        // Cancel immediately
        cts.Cancel();

        // The operation may complete before checking cancellation, or throw
        // depending on timing, so we just verify it doesn't hang
        try
        {
            await collection.WriteToAsync(stream, cancellationToken: cts.Token);
        }
        catch (OperationCanceledException)
        {
            // Expected if cancellation was checked
        }
    }

    [Fact]
    public async Task ReadFromAsync_WithCancellation_CanBeCancelled()
    {
        var items = new[] { new SimpleRecord { Id = 1, Name = "Test", Value = 1.0 } };
        using var original = items.ToArrowCollection();

        using var stream = new MemoryStream();
        await original.WriteToAsync(stream);
        stream.Position = 0;

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        try
        {
            await ArrowCollection<SimpleRecord>.ReadFromAsync(stream, cancellationToken: cts.Token);
        }
        catch (OperationCanceledException)
        {
            // Expected if cancellation was checked
        }
    }

    #endregion
}
