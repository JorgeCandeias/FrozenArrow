using Apache.Arrow;
using Apache.Arrow.Types;
using FrozenArrow.Query;
using Xunit;

namespace FrozenArrow.Tests.Rendering;

public class ArrowIpcRenderingTests
{
    [ArrowRecord]
    public class Person
    {
        [ArrowArray]
        public string Name { get; set; } = string.Empty;
        
        [ArrowArray]
        public int Age { get; set; }
        
        [ArrowArray]
        public string? Status { get; set; }
    }

    [Fact]
    public void ToArrowBatch_FullScan_ReturnsOriginalBatch()
    {
        // Arrange
        var people = new[]
        {
            new Person { Name = "Alice", Age = 30, Status = "Active" },
            new Person { Name = "Bob", Age = 25, Status = "Active" },
            new Person { Name = "Charlie", Age = 35, Status = "Inactive" }
        };

        using var collection = people.ToFrozenArrow();

        // Act
        var batch = collection
            .AsQueryable()
            .ToArrowBatch();

        // Assert
        Assert.Equal(3, batch.Length);
        Assert.Equal(3, batch.ColumnCount); // Name, Age, Status
    }

    [Fact]
    public void ToArrowBatch_WithFilter_ReturnsFilteredBatch()
    {
        // Arrange
        var people = new[]
        {
            new Person { Name = "Alice", Age = 30, Status = "Active" },
            new Person { Name = "Bob", Age = 25, Status = "Active" },
            new Person { Name = "Charlie", Age = 35, Status = "Inactive" },
            new Person { Name = "David", Age = 40, Status = "Active" },
            new Person { Name = "Eve", Age = 28, Status = "Inactive" }
        };

        using var collection = people.ToFrozenArrow();

        // Act - Filter Age > 30
        var batch = collection
            .AsQueryable()
            .Where(p => p.Age > 30)
            .ToArrowBatch();

        // Assert
        Assert.Equal(2, batch.Length); // Charlie (35) and David (40)
        
        // Verify the actual data
        var ageColumn = (Int32Array)batch.Column(batch.Schema.GetFieldIndex("Age"));
        Assert.Equal(35, ageColumn.GetValue(0));
        Assert.Equal(40, ageColumn.GetValue(1));
    }

    [Fact]
    public void ToArrowBatch_WithMultipleFilters_ReturnsFilteredBatch()
    {
        // Arrange
        var people = new[]
        {
            new Person { Name = "Alice", Age = 30, Status = "Active" },
            new Person { Name = "Bob", Age = 25, Status = "Active" },
            new Person { Name = "Charlie", Age = 35, Status = "Inactive" },
            new Person { Name = "David", Age = 40, Status = "Active" },
            new Person { Name = "Eve", Age = 28, Status = "Inactive" }
        };

        using var collection = people.ToFrozenArrow();

        // Act - Filter Age > 25 AND Status == "Active"
        var batch = collection
            .AsQueryable()
            .Where(p => p.Age > 25 && p.Status == "Active")
            .ToArrowBatch();

        // Assert
        Assert.Equal(2, batch.Length); // Alice (30) and David (40)
        
        // Verify the actual data - use helper to handle both StringArray and DictionaryArray
        var nameColumn = batch.Column(batch.Schema.GetFieldIndex("Name"));
        Assert.Equal("Alice", GetStringValue(nameColumn, 0));
        Assert.Equal("David", GetStringValue(nameColumn, 1));
    }

    // Helper method to get string value regardless of encoding (StringArray or DictionaryArray)
    private static string? GetStringValue(IArrowArray array, int index)
    {
        if (array.IsNull(index))
            return null;

        return array switch
        {
            StringArray stringArray => stringArray.GetString(index),
            DictionaryArray dictArray => GetStringValue(dictArray.Dictionary, GetDictionaryIndex(dictArray.Indices, index)),
            _ => throw new NotSupportedException($"Unsupported array type for string access: {array.GetType().Name}")
        };
    }

    // Helper to extract dictionary index from various Arrow integer array types
    private static int GetDictionaryIndex(IArrowArray indices, int rowIndex)
    {
        return indices switch
        {
            Int8Array int8 => int8.GetValue(rowIndex)!.Value,
            UInt8Array uint8 => uint8.GetValue(rowIndex)!.Value,
            Int16Array int16 => int16.GetValue(rowIndex)!.Value,
            UInt16Array uint16 => (int)uint16.GetValue(rowIndex)!.Value,
            Int32Array int32 => int32.GetValue(rowIndex)!.Value,
            UInt32Array uint32 => checked((int)uint32.GetValue(rowIndex)!.Value),
            Int64Array int64 => checked((int)int64.GetValue(rowIndex)!.Value),
            UInt64Array uint64 => checked((int)uint64.GetValue(rowIndex)!.Value),
            _ => throw new NotSupportedException($"Unsupported dictionary index array type: {indices.GetType().Name}")
        };
    }

    [Fact]
    public void ToArrowBatch_EmptyResult_ReturnsEmptyBatch()
    {
        // Arrange
        var people = new[]
        {
            new Person { Name = "Alice", Age = 30, Status = "Active" },
            new Person { Name = "Bob", Age = 25, Status = "Active" }
        };

        using var collection = people.ToFrozenArrow();

        // Act - Filter that matches nothing
        var batch = collection
            .AsQueryable()
            .Where(p => p.Age > 100)
            .ToArrowBatch();

        // Assert
        Assert.Equal(0, batch.Length);
        Assert.Equal(3, batch.ColumnCount); // Schema preserved, no data
    }

    [Fact]
    public void WriteArrowIpc_WithFilter_WritesFilteredData()
    {
        // Arrange
        var people = new[]
        {
            new Person { Name = "Alice", Age = 30, Status = "Active" },
            new Person { Name = "Bob", Age = 25, Status = "Active" },
            new Person { Name = "Charlie", Age = 35, Status = "Inactive" }
        };

        using var collection = people.ToFrozenArrow();
        using var stream = new MemoryStream();

        // Act - Write filtered data
        collection
            .AsQueryable()
            .Where(p => p.Status == "Active")
            .WriteArrowIpc(stream, leaveOpen: true);

        // Assert - Read back and verify
        stream.Position = 0;
        using var reader = new Apache.Arrow.Ipc.ArrowStreamReader(stream);
        var readBatch = reader.ReadNextRecordBatch();

        Assert.NotNull(readBatch);
        Assert.Equal(2, readBatch.Length); // Alice and Bob

        var nameColumn = readBatch.Column(readBatch.Schema.GetFieldIndex("Name"));
        Assert.Equal("Alice", GetStringValue(nameColumn, 0));
        Assert.Equal("Bob", GetStringValue(nameColumn, 1));
    }

    [Fact]
    public void ToArrowBatch_PreservesNulls()
    {
        // Arrange
        var people = new[]
        {
            new Person { Name = "Alice", Age = 30, Status = "Active" },
            new Person { Name = "Bob", Age = 25, Status = null }, // Null status
            new Person { Name = "Charlie", Age = 35, Status = "Active" }
        };

        using var collection = people.ToFrozenArrow();

        // Act
        var batch = collection
            .AsQueryable()
            .ToArrowBatch();

        // Assert
        // Note: String columns may be encoded as StringArray or DictionaryArray depending on cardinality
        var statusColumn = batch.Column(batch.Schema.GetFieldIndex("Status"));
        Assert.False(statusColumn.IsNull(0)); // Alice - not null
        Assert.True(statusColumn.IsNull(1));  // Bob - null!
        Assert.False(statusColumn.IsNull(2)); // Charlie - not null
    }

    [Fact]
    public void ToArrowBatch_LargeDataset_PerformsEfficiently()
    {
        // Arrange - Create large dataset
        var people = Enumerable.Range(0, 100_000)
            .Select(i => new Person 
            { 
                Name = $"Person{i}", 
                Age = 20 + (i % 60), 
                Status = i % 2 == 0 ? "Active" : "Inactive" 
            })
            .ToArray();

        using var collection = people.ToFrozenArrow();

        // Act - Filter ~50% of data
        var batch = collection
            .AsQueryable()
            .Where(p => p.Status == "Active")
            .ToArrowBatch();

        // Assert - Verify correctness (avoid timing assertions which are flaky across machines/CI)
        Assert.Equal(50_000, batch.Length);
        Assert.Equal(3, batch.ColumnCount); // Name, Age, Status
    }
}

