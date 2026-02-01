# ArrowCollection - Frozen Collection with Apache Arrow Compression

ArrowCollection is a .NET library that implements a frozen generic collection with columnar compression using Apache Arrow. It's designed for scenarios where you need significant in-memory compression savings for massive datasets, while accepting the performance trade-off of reconstructing items on-the-fly during enumeration.

## Features

- **Immutable/Frozen**: Once created, the collection cannot be modified
- **Columnar Compression**: Uses Apache Arrow format for efficient compression
- **Type-Safe**: Strongly typed generic collection
- **Simple API**: Easy to use with the `.ToArrowCollection()` extension method
- **Source Generator**: Compile-time code generation for optimal performance
- **IDisposable**: Properly releases unmanaged Arrow memory when disposed
- **Serialization**: Read/write to streams and buffers using Arrow IPC format
- **Schema Evolution**: Name-based column matching with configurable validation
- **Positional Records**: Full support for C# records without parameterless constructors

## Installation

Add the ArrowCollection library to your project:

```bash
dotnet add reference path/to/ArrowCollection/ArrowCollection.csproj
```

The library includes an embedded source generator that processes your types at compile time.

## Usage

### Basic Example

Types must be decorated with `[ArrowRecord]` at the class level, and each field/property to include must be decorated with `[ArrowArray]`:

```csharp
using ArrowCollection;

[ArrowRecord]
public class Person
{
    [ArrowArray]
    public int Id { get; set; }
    [ArrowArray]
    public string Name { get; set; } = string.Empty;
    [ArrowArray]
    public int Age { get; set; }
    [ArrowArray]
    public DateTime BirthDate { get; set; }
}

// Create your data
var people = new[]
{
    new Person { Id = 1, Name = "Alice", Age = 30, BirthDate = new DateTime(1994, 5, 15) },
    new Person { Id = 2, Name = "Bob", Age = 25, BirthDate = new DateTime(1999, 8, 22) },
    new Person { Id = 3, Name = "Charlie", Age = 35, BirthDate = new DateTime(1989, 3, 10) }
};

// Convert to ArrowCollection (frozen collection)
// Remember to dispose when done to release unmanaged memory
using var collection = people.ToArrowCollection();

// Enumerate the collection (items are materialized on-the-fly)
foreach (var person in collection)
{
    Console.WriteLine($"{person.Name} is {person.Age} years old");
}

// Get count
Console.WriteLine($"Total people: {collection.Count}");
```

### Large Dataset Example

```csharp
// Create a large dataset
var largeDataset = Enumerable.Range(1, 1_000_000)
    .Select(i => new Person 
    { 
        Id = i, 
        Name = $"Person {i}", 
        Age = 20 + (i % 60),
        BirthDate = DateTime.Now.AddYears(-(20 + (i % 60)))
    });

// Convert to ArrowCollection - data is compressed using Apache Arrow columnar format
using var collection = largeDataset.ToArrowCollection();

// The data is now stored in a compressed columnar format
// Items are reconstructed on-the-fly during enumeration
var adults = collection.Where(p => p.Age >= 18).Take(10);
```

### Supported Data Types

ArrowCollection supports the following property/field types:

- **Signed Integers**: `int`, `long`, `short`, `sbyte`
- **Unsigned Integers**: `uint`, `ulong`, `ushort`, `byte`
- **Floating Point**: `float`, `double`, `Half`
- **Decimal**: `decimal` (stored as Arrow Decimal128 with precision 29, scale 6)
- **Boolean**: `bool`
- **String**: `string`
- **Binary**: `byte[]` (variable-length binary data)
- **DateTime**: `DateTime` (stored as UTC timestamps in milliseconds)
- **Nullable versions**: All of the above types can be nullable (`int?`, `string?`, `DateTime?`, `Half?`, `decimal?`, `byte[]?`, etc.)

### Working with Nullable Properties

```csharp
[ArrowRecord]
public class OptionalData
{
    [ArrowArray]
    public int? OptionalId { get; set; }
    [ArrowArray]
    public string? OptionalName { get; set; }
    [ArrowArray]
    public DateTime? OptionalDate { get; set; }
}

var data = new[]
{
    new OptionalData { OptionalId = 1, OptionalName = "Test", OptionalDate = DateTime.Now },
    new OptionalData { OptionalId = null, OptionalName = null, OptionalDate = null },
};

using var collection = data.ToArrowCollection();
```

### Working with Half-Precision Floats and Binary Data

ArrowCollection supports `Half` (half-precision floating point) and `byte[]` (variable-length binary data):

```csharp
[ArrowRecord]
public class ScientificData
{
    [ArrowArray]
    public int Id { get; set; }
    
    [ArrowArray]
    public Half LowPrecisionValue { get; set; }  // 16-bit float for memory efficiency
    
    [ArrowArray]
    public byte[] RawData { get; set; } = [];    // Variable-length binary data
    
    [ArrowArray]
    public Half? OptionalMeasurement { get; set; }
    
    [ArrowArray]
    public byte[]? OptionalPayload { get; set; }
}

var readings = new[]
{
    new ScientificData 
    { 
        Id = 1, 
        LowPrecisionValue = (Half)3.14f, 
        RawData = new byte[] { 0x01, 0x02, 0x03 },
        OptionalMeasurement = (Half)2.71f,
        OptionalPayload = new byte[] { 0xFF }
    },
    new ScientificData 
    { 
        Id = 2, 
        LowPrecisionValue = (Half)1.5f, 
        RawData = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF },
        OptionalMeasurement = null,
        OptionalPayload = null
    }
};

using var collection = readings.ToArrowCollection();
```

### Using Fields Instead of Properties

You can also use the `[ArrowArray]` attribute directly on fields:

```csharp
[ArrowRecord]
public class FieldBasedItem
{
    [ArrowArray]
    public int Id;
    [ArrowArray]
    public string Name = string.Empty;
}
```

**Note**: Manual properties (properties with custom getter/setter logic) are **not supported**. The `[ArrowArray]` attribute only works on:
- Auto-properties (the compiler-generated backing field is accessed directly)
- Fields (accessed directly)

### Workaround for Manual Properties

If you need custom logic in your property getter or setter, annotate a backing field with `[ArrowArray]` and leave the property without the attribute:

```csharp
[ArrowRecord]
public class ItemWithManualProperty
{
    [ArrowArray]
    private string _name = string.Empty;

    // Manual property with custom logic - NOT annotated
    public string Name
    {
        get => _name;
        set => _name = value?.Trim() ?? string.Empty;
    }

    [ArrowArray]
    public int Id { get; set; }
}
```

In this example, the `_name` field is stored in the Arrow format, while the `Name` property provides the custom getter/setter logic. When items are reconstructed during enumeration, the field is populated directly, bypassing the property setter.

### Using Structs

ArrowCollection supports both classes and structs, including readonly structs:

```csharp
// Mutable struct
[ArrowRecord]
public struct DataPoint
{
    [ArrowArray]
    public int Id { get; set; }
    [ArrowArray]
    public double Value { get; set; }
    [ArrowArray]
    public DateTime Timestamp { get; set; }
}

// Readonly struct (uses init-only setters)
[ArrowRecord]
public readonly struct ImmutableDataPoint
{
    [ArrowArray]
    public int Id { get; init; }
    [ArrowArray]
    public double Value { get; init; }
    [ArrowArray]
    public DateTime Timestamp { get; init; }
}

// Usage is identical to classes
var dataPoints = new[]
{
    new DataPoint { Id = 1, Value = 10.5, Timestamp = DateTime.UtcNow },
    new DataPoint { Id = 2, Value = 20.5, Timestamp = DateTime.UtcNow }
};

using var collection = dataPoints.ToArrowCollection();

foreach (var point in collection)
{
    Console.WriteLine($"Point {point.Id}: {point.Value}");
}
```

**Note**: For structs, the library uses ref-based IL emission to set fields without copying, ensuring optimal performance even for readonly structs.

### Using Positional Records

ArrowCollection fully supports C# positional records (both `record class` and `record struct`) without requiring a parameterless constructor:

```csharp
// Positional record class - no parameterless constructor needed!
[ArrowRecord]
public record Person(
    [property: ArrowArray] int Id,
    [property: ArrowArray] string Name,
    [property: ArrowArray] double Salary);

// Positional record struct
[ArrowRecord]
public record struct Point(
    [property: ArrowArray] int X,
    [property: ArrowArray] int Y);

// Readonly positional record struct
[ArrowRecord]
public readonly record struct ImmutablePoint(
    [property: ArrowArray] double X,
    [property: ArrowArray] double Y);

// Usage
var people = new[]
{
    new Person(1, "Alice", 50000.0),
    new Person(2, "Bob", 60000.0)
};

using var collection = people.ToArrowCollection();

foreach (var person in collection)
{
    Console.WriteLine($"{person.Name} earns {person.Salary:C}");
}
```

You can also mix positional parameters with additional properties:

```csharp
[ArrowRecord]
public record Employee(
    [property: ArrowArray] int Id,
    [property: ArrowArray] string Name)
{
    [ArrowArray]
    public DateTime HireDate { get; init; }
    
    [ArrowArray]
    public decimal Salary { get; init; }
}
```

> **Note**: For positional records, use the `[property: ArrowArray]` syntax to apply the attribute to the generated property. This is standard C# syntax for targeting attributes to specific elements.

## Important: Frozen Collection Semantics

ArrowCollection is a **frozen collection** by design:

- **Immutable after creation**: Once built, no items can be added, removed, or modified
- **Data is copied on construction**: The source data is copied into Arrow columnar format during `ToArrowCollection()`
- **Items are reconstructed on enumeration**: Each enumeration creates new instances from the stored columnar data
- **Original source independence**: Changes to the original source collection have no effect on the ArrowCollection
- **Reconstructed item independence**: Modifying items obtained during enumeration has no effect on the stored data
- **Constructors are bypassed**: Items are created without calling constructors; fields are set directly

This frozen design enables:
- Thread-safe reading without locks
- Consistent data regardless of original source mutations
- Optimizations based on immutability guarantees
- Support for positional records without parameterless constructors

### Constructor Bypass Behavior

ArrowCollection uses a technique similar to [Orleans serialization](https://learn.microsoft.com/en-us/dotnet/orleans/host/configuration-guide/serialization) where constructors are intentionally bypassed during item reconstruction:

- **Classes**: Created using `RuntimeHelpers.GetUninitializedObject` (no constructor called)
- **Structs**: Created using `default(T)` (no boxing overhead)
- **Fields**: Set directly via generated code, bypassing property setters

This means:
- ✅ Positional records work without a parameterless constructor
- ✅ Readonly fields and init-only properties are fully supported
- ⚠️ Constructor validation logic is **not** executed
- ⚠️ Field initializers are **not** executed

This behavior is by design, as ArrowCollection expects types to be **pure data containers** without logic in constructors or property setters.

## Serialization

ArrowCollection supports binary serialization using Apache Arrow IPC format, enabling persistence and data transfer while preserving the efficient columnar structure.

### Writing to Storage

```csharp
using System.Buffers;

// Create a collection
var items = new[]
{
    new Person { Id = 1, Name = "Alice", Age = 30 },
    new Person { Id = 2, Name = "Bob", Age = 25 }
};

using var collection = items.ToArrowCollection();

// Write to a stream (async)
using var fileStream = File.Create("data.arrow");
await collection.WriteToAsync(fileStream);

// Or write to a buffer (sync, high-performance)
var buffer = new ArrayBufferWriter<byte>();
collection.WriteTo(buffer);
```

### Reading from Storage

```csharp
// Read from a stream (async)
using var fileStream = File.OpenRead("data.arrow");
using var collection = await ArrowCollection<Person>.ReadFromAsync(fileStream);

// Or read from a span (sync)
byte[] data = File.ReadAllBytes("data.arrow");
using var collection = ArrowCollection<Person>.ReadFrom(data.AsSpan());

// Or read from a ReadOnlySequence (for pipeline scenarios)
var sequence = new ReadOnlySequence<byte>(data);
using var collection = ArrowCollection<Person>.ReadFrom(sequence);
```

### Schema Evolution

ArrowCollection supports forward-compatible schema evolution through name-based column matching:

```csharp
// Original model (v1)
[ArrowRecord]
public class PersonV1
{
    [ArrowArray(Name = "id")]
    public int Id { get; set; }
    
    [ArrowArray(Name = "name")]
    public string Name { get; set; } = string.Empty;
}

// Updated model (v2) - added new field
[ArrowRecord]
public class PersonV2
{
    [ArrowArray(Name = "id")]
    public int Id { get; set; }
    
    [ArrowArray(Name = "name")]
    public string Name { get; set; } = string.Empty;
    
    [ArrowArray(Name = "email")]
    public string? Email { get; set; }  // New field - will get default value when reading v1 data
}
```

### Validation Options

Control how schema mismatches are handled:

```csharp
// Strict validation - throw on any schema mismatch
var strictOptions = new ArrowReadOptions
{
    UnknownColumns = UnknownColumnBehavior.Throw,  // Throw if source has extra columns
    MissingColumns = MissingColumnBehavior.Throw   // Throw if source is missing columns
};

using var collection = await ArrowCollection<Person>.ReadFromAsync(stream, strictOptions);

// Lenient validation (default) - ignore extra columns, use defaults for missing
var lenientOptions = new ArrowReadOptions
{
    UnknownColumns = UnknownColumnBehavior.Ignore,   // Silently skip unknown columns
    MissingColumns = MissingColumnBehavior.UseDefault // Use default(T) for missing columns
};
```

### Explicit Column Names

Use the `Name` property on `[ArrowArray]` to decouple serialization names from code:

```csharp
[ArrowRecord]
public class Customer
{
    // Rename the property without breaking existing serialized data
    [ArrowArray(Name = "customer_id")]
    public int CustomerId { get; set; }
    
    // Field names should always specify Name for stable serialization
    [ArrowArray(Name = "internal_score")]
    private double _score;
}
```

> **Note**: The source generator emits warning `ARROWCOL005` when `[ArrowArray]` is applied to a field without an explicit `Name`. This helps catch potential serialization issues with field naming conventions (e.g., `_fieldName`).

## What's Not Supported

- **Complex types**: Nested objects, collections, arrays, enums, or custom structs as field types
- **Manual properties**: Properties with custom getter/setter implementations
- **Types without parameterless constructors**: The target type must have a public parameterless constructor (structs have this implicitly)
- **Indexer access**: No direct index-based access to items (use LINQ `.ElementAt()` if needed)

## Diagnostic Messages

The source generator produces helpful diagnostic messages:

| Code | Severity | Description |
|------|----------|-------------|
| `ARROWCOL001` | Error | Type has `[ArrowRecord]` but no properties/fields marked with `[ArrowArray]` |
| `ARROWCOL002` | Error | Property/field has an unsupported type |
| `ARROWCOL003` | Error | Type is missing a public parameterless constructor |
| `ARROWCOL004` | Error | `[ArrowArray]` on a manual property (not an auto-property) |
| `ARROWCOL005` | Warning | Field has `[ArrowArray]` but no explicit `Name` specified |

## Performance Characteristics

### Advantages
- **Memory Efficiency**: Significant compression for large datasets using Apache Arrow's columnar format
- **Multiple Enumerations**: Can enumerate the collection multiple times
- **Immutability**: Thread-safe for reading (data is frozen after creation)
- **Source-Generated**: Zero reflection at runtime for item creation (IL-emitted field accessors)
- **Efficient Serialization**: Arrow IPC format preserves columnar structure for fast I/O

### Trade-offs
- **Enumeration Cost**: Items are reconstructed on-the-fly, which is slower than iterating in-memory objects
- **Not for Frequent Access**: Best suited for scenarios where data is enumerated infrequently but needs to be kept in memory
- **Construction Cost**: Initial creation requires copying all data into Arrow format

## Use Cases

ArrowCollection is ideal for:

- **Caching large datasets** that are infrequently accessed
- **In-memory analytics** where memory is constrained
- **Reference data** that needs to be kept in memory but rarely accessed
- **Historical data** that must be available but isn't frequently queried
- **Data persistence** with efficient columnar storage format
- **Cross-language interop** via Arrow IPC format (Python, Rust, Java, etc.)

## Project Structure

```
ArrowCollection/
├── src/
│   ├── ArrowCollection/              # Core library
│   └── ArrowCollection.Generators/   # Source generator
├── tests/
│   └── ArrowCollection.Tests/        # Unit tests
├── benchmarks/
│   └── ArrowCollection.Benchmarks/   # Performance benchmarks
└── samples/
    └── ArrowCollection.Sample/       # Sample application
```

## Requirements

- .NET 10.0 or later
- Apache.Arrow NuGet package (automatically included)

## License

See LICENSE file for details.
