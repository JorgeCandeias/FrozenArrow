# FrozenArrow

### Frozen Collection with Apache Arrow Compression for .NET

> **Trademark Notice**: "Apache Arrow" and "Arrow" are trademarks of [The Apache Software Foundation](https://www.apache.org/). FrozenArrow is an independent, community-driven project and is not affiliated with, endorsed by, or sponsored by The Apache Software Foundation. The name "FrozenArrow" is used purely to describe the library's functionality: providing .NET collections backed by the Apache Arrow columnar format.

FrozenArrow is a .NET library that implements a frozen generic collection with columnar compression using Apache Arrow. It's designed for scenarios where you need significant in-memory compression savings for massive datasets, while accepting the performance trade-off of reconstructing items on-the-fly during enumeration.

## Features


- **Immutable/Frozen**: Once created, the collection cannot be modified
- **Columnar Compression**: Uses Apache Arrow format for efficient compression
- **Type-Safe**: Strongly typed generic collection
- **Simple API**: Easy to use with the `.ToFrozenArrow()` extension method
- **Source Generator**: Compile-time code generation for optimal performance
- **IDisposable**: Properly releases unmanaged Arrow memory when disposed
- **Serialization**: Read/write to streams and buffers using Arrow IPC format
- **IPC Compression**: Optional LZ4 and Zstd compression for serialized data
- **Schema Evolution**: Name-based column matching with configurable validation
- **Positional Records**: Full support for C# records without parameterless constructors
- **Optimized LINQ Queries**: `IQueryable<T>` implementation with column-level predicate pushdown
- **Compile-Time Diagnostics**: Roslyn analyzer catches inefficient query patterns

## Installation

Add the FrozenArrow library to your project:

```bash
dotnet add reference path/to/FrozenArrow/FrozenArrow.csproj
```

The library includes an embedded source generator that processes your types at compile time.

## Usage

### Basic Example

Types must be decorated with `[ArrowRecord]` at the class level, and each field/property to include must be decorated with `[ArrowArray]`:

```csharp
using FrozenArrow;

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

// Convert to FrozenArrow (frozen collection)
// Remember to dispose when done to release unmanaged memory
using var collection = people.ToFrozenArrow();

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

// Convert to FrozenArrow - data is compressed using Apache Arrow columnar format
using var collection = largeDataset.ToFrozenArrow();

// INEFFICIENT: This enumerates all 1M items, creating 1M Person objects
var adultsOld = collection.Where(p => p.Age >= 18).Take(10);

// OPTIMIZED: Use AsQueryable() for column-level filtering
// Only the Age column is scanned, and only matching rows are materialized
var adultsOptimized = collection
    .AsQueryable()
    .Where(p => p.Age >= 18)
    .Take(10)
    .ToList();
```

### Supported Data Types

FrozenArrow supports the following property/field types:

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

using var collection = data.ToFrozenArrow();
```

### Working with Half-Precision Floats and Binary Data

FrozenArrow supports `Half` (half-precision floating point) and `byte[]` (variable-length binary data):

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

using var collection = readings.ToFrozenArrow();
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

FrozenArrow supports both classes and structs, including readonly structs:

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

using var collection = dataPoints.ToFrozenArrow();

foreach (var point in collection)
{
    Console.WriteLine($"Point {point.Id}: {point.Value}");
}
```

**Note**: For structs, the library uses ref-based IL emission to set fields without copying, ensuring optimal performance even for readonly structs.

### Using Positional Records

FrozenArrow fully supports C# positional records (both `record class` and `record struct`) without requiring a parameterless constructor:

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

using var collection = people.ToFrozenArrow();

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

FrozenArrow is a **frozen collection** by design:

- **Immutable after creation**: Once built, no items can be added, removed, or modified
- **Data is copied on construction**: The source data is copied into Arrow columnar format during `ToFrozenArrow()`
- **Items are reconstructed on enumeration**: Each enumeration creates new instances from the stored columnar data
- **Original source independence**: Changes to the original source collection have no effect on the FrozenArrow
- **Reconstructed item independence**: Modifying items obtained during enumeration has no effect on the stored data
- **Constructors are bypassed**: Items are created without calling constructors; fields are set directly

This frozen design enables:
- Thread-safe reading without locks
- Consistent data regardless of original source mutations
- Optimizations based on immutability guarantees
- Support for positional records without parameterless constructors

### Constructor Bypass Behavior

FrozenArrow uses a technique similar to [Orleans serialization](https://learn.microsoft.com/en-us/dotnet/orleans/host/configuration-guide/serialization) where constructors are intentionally bypassed during item reconstruction:

- **Classes**: Created using `RuntimeHelpers.GetUninitializedObject` (no constructor called)
- **Structs**: Created using `default(T)` (no boxing overhead)
- **Fields**: Set directly via generated code, bypassing property setters

This means:
- ✅ Positional records work without a parameterless constructor
- ✅ Readonly fields and init-only properties are fully supported
- ⚠️ Constructor validation logic is **not** executed
- ⚠️ Field initializers are **not** executed

This behavior is by design, as FrozenArrow expects types to be **pure data containers** without logic in constructors or property setters.

## Serialization

FrozenArrow supports binary serialization using Apache Arrow IPC format, enabling persistence and data transfer while preserving the efficient columnar structure.

### Writing to Storage

```csharp
using System.Buffers;
using Apache.Arrow.Ipc;

// Create a collection
var items = new[]
{
    new Person { Id = 1, Name = "Alice", Age = 30 },
    new Person { Id = 2, Name = "Bob", Age = 25 }
};

using var collection = items.ToFrozenArrow();

// Write to a stream (async) - no compression
using var fileStream = File.Create("data.arrow");
await collection.WriteToAsync(fileStream);

// Write with LZ4 compression (fast, good compression)
var lz4Options = new ArrowWriteOptions { CompressionCodec = CompressionCodecType.Lz4Frame };
await collection.WriteToAsync(fileStream, lz4Options);

// Write with Zstd compression (slower, better compression)
var zstdOptions = new ArrowWriteOptions { CompressionCodec = CompressionCodecType.Zstd };
await collection.WriteToAsync(fileStream, zstdOptions);

// Or write to a buffer (sync, high-performance)
var buffer = new ArrayBufferWriter<byte>();
collection.WriteTo(buffer, lz4Options);
```

### Reading from Storage

```csharp
// Read from a stream (async) - compression is auto-detected
using var fileStream = File.OpenRead("data.arrow");
using var collection = await FrozenArrow<Person>.ReadFromAsync(fileStream);

// Or read from a span (sync)
byte[] data = File.ReadAllBytes("data.arrow");
using var collection = FrozenArrow<Person>.ReadFrom(data.AsSpan());

// Or read from a ReadOnlySequence (for pipeline scenarios)
var sequence = new ReadOnlySequence<byte>(data);
using var collection = FrozenArrow<Person>.ReadFrom(sequence);
```

### Schema Evolution

FrozenArrow supports forward-compatible schema evolution through name-based column matching:

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

using var collection = await FrozenArrow<Person>.ReadFromAsync(stream, strictOptions);

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
- **Indexer access**: No direct index-based access to items (use LINQ `.ElementAt()` if needed)

## Optimized LINQ Queries with ArrowQuery

FrozenArrow includes a powerful query engine that enables **optimized LINQ queries** directly against the columnar Arrow data, without materializing objects until needed.

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            User Code                                        │
│  collection.AsQueryable().Where(x => x.Age > 30).Where(x => x.IsActive)     │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         ArrowQuery<T>                                       │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ • Implements IQueryable<T> for transparent LINQ integration         │    │
│  │ • Captures expressions without immediate execution                  │    │
│  │ • Validates operations at query build time                          │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                             │
│  Supported: Where, Select, First, Any, All, Count, Take, Skip, OrderBy      │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ (on enumeration)
┌─────────────────────────────────────────────────────────────────────────────┐
│                      Query Execution Pipeline                               │
│                                                                             │
│  1. Parse predicates → Extract column-level filters                         │
│  2. Build selection bitmap → Evaluate directly on Arrow columns             │
│  3. Yield items at selected indices → Materialize only matching rows        │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Basic Query Usage

```csharp
using FrozenArrow.Query;

// Create a queryable view over the collection
var results = collection
    .AsQueryable()
    .Where(x => x.Age > 30 && x.Category == "Engineering")
    .Where(x => x.IsActive)
    .ToList();

// Or use the Query() method for access to additional features
var query = collection.Query();
Console.WriteLine(query.Explain()); // Shows the query execution plan
```

### How It Works: Column-Level Filtering

The key insight is that **predicates are evaluated directly against Arrow columns** without materializing full objects:

```
Traditional LINQ (1M rows, 20 columns):
┌─────────────────────────────────────────────────┐
│ For each row:                                   │
│   1. Create Person object (20 field copies)     │
│   2. Evaluate predicate: person.Age > 30        │
│   3. If true, add to results                    │
│                                                 │
│ Memory: ~400 MB (1M objects × ~400 bytes each)  │
│ Objects created: 1,000,000                      │
└─────────────────────────────────────────────────┘

ArrowQuery (1M rows, 20 columns):
┌─────────────────────────────────────────────────┐
│ 1. Scan Age column only → build selection mask  │
│ 2. For selected indices only:                   │
│    Create Person object                         │
│                                                 │
│ Memory: ~4 MB (Age column) + results only       │
│ Objects created: ~300,000 (matching rows)       │
└─────────────────────────────────────────────────┘
```

### Supported Predicates

ArrowQuery can push these predicates to column-level evaluation:

| Pattern | Example | Description |
|---------|---------|-------------|
| Numeric comparison | `x => x.Age > 30` | `<`, `>`, `<=`, `>=`, `==`, `!=` |
| String equality | `x => x.Name == "Alice"` | Case-sensitive by default |
| String operations | `x => x.Name.Contains("a")` | `Contains`, `StartsWith`, `EndsWith` |
| Boolean property | `x => x.IsActive` | Direct boolean check |
| Negated boolean | `x => !x.IsActive` | Negated boolean check |
| Null check | `x => x.Name != null` | Null/not-null comparison |
| AND combination | `x => x.Age > 30 && x.IsActive` | Split into multiple column filters |
| Captured variables | `x => x.Age > minAge` | Variables from outer scope |

### Query Plan Inspection

Use `Explain()` to understand how your query will be executed:

```csharp
var query = collection
    .AsQueryable()
    .Where(x => x.Age > 30 && x.Category == "Premium");

Console.WriteLine(query.Explain());
// Output:
// Query Plan (Optimized: True)
//   Columns: Age, Category
//   Predicates: 2 column-level filter(s)
//   Est. Selectivity: 9%
```

### Strict Mode vs Fallback Mode

By default, ArrowQuery operates in **strict mode** and throws `NotSupportedException` for operations that cannot be optimized:

```csharp
// Strict mode (default) - throws on unsupported operations
var results = collection
    .AsQueryable()
    .Where(x => x.Age > 30)
    .ToList(); // ✓ Supported

// This would throw NotSupportedException:
// .Where(x => ComputeSomething(x.Age))  // ✗ External method call

// Allow fallback to full materialization (use with caution!)
var results = collection
    .AsQueryable()
    .AllowFallback()  // Disables strict mode
    .Where(x => ComputeSomething(x.Age))
    .ToList();
```

### Supported LINQ Methods

| Method | Optimized | Notes |
|--------|-----------|-------|
| `Where` | ✓ | Predicates pushed to column-level |
| `First`, `FirstOrDefault` | ✓ | Stops at first match |
| `Single`, `SingleOrDefault` | ✓ | Validates single result |
| `Any` | ✓ | Short-circuits on first match |
| `All` | ✓ | Short-circuits on first non-match |
| `Count`, `LongCount` | ✓ | Counts selection bitmap |
| `Take`, `Skip` | ✓ | Pagination support |
| `OrderBy`, `OrderByDescending` | Partial | Sorts matching results |
| `Select` | Partial | Column projection |
| `Sum`, `Average`, `Min`, `Max` | ✓ | Column-level aggregates (no materialization) |
| `GroupBy` + aggregates | ✓ | Column-level grouping with aggregates |
| `GroupBy` + `ToDictionary` | ✓ | Returns Dictionary with aggregated values |
| `ToList`, `ToArray` | ✓ | Materializes results |

### Column-Level Aggregations

ArrowQuery can compute aggregates directly on columns without materializing any objects:

```csharp
// Single aggregate - computed directly on Age column
var avgAge = collection
    .AsQueryable()
    .Where(x => x.IsActive)
    .Average(x => x.Age);  // No objects created!

// Multiple aggregates in a single pass
var stats = collection
    .AsQueryable()
    .Where(x => x.IsActive)
    .Aggregate(agg => new
    {
        TotalSalary = agg.Sum(x => x.Salary),
        AverageAge = agg.Average(x => x.Age),
        MinSalary = agg.Min(x => x.Salary),
        MaxSalary = agg.Max(x => x.Salary),
        Count = agg.Count()
    });
// All 5 aggregates computed in ONE pass over the data!
```

### GroupBy with Aggregations

Grouped aggregations are computed at the column level:


```csharp
// Group by a column and compute aggregates per group
var summary = collection
    .AsQueryable()
    .Where(x => x.IsActive)
    .GroupBy(x => x.Age)
    .Select(g => new
    {
        Age = g.Key,
        Count = g.Count(),
        TotalSalary = g.Sum(x => x.Salary),
        AvgPerformance = g.Average(x => x.PerformanceScore)
    })
    .ToList();

// Also works with concrete result types:
var results = collection
    .AsQueryable()
    .GroupBy(x => x.Category)
    .Select(g => new CategorySummary
    {
        Key = g.Key,
        Count = g.Count(),
        TotalSalary = g.Sum(x => x.Salary)
    })
    .ToList();
```

### GroupBy with ToDictionary

For cases where you want to return grouped aggregates as a dictionary, use the optimized `ToDictionary` pattern:

```csharp
// Count per category as a dictionary
Dictionary<string, int> countByCategory = collection
    .AsQueryable()
    .GroupBy(x => x.Category)
    .ToDictionary(g => g.Key, g => g.Count());

// Sum of salaries per department
Dictionary<string, decimal> salaryByDept = collection
    .AsQueryable()
    .GroupBy(x => x.Department)
    .ToDictionary(g => g.Key, g => g.Sum(x => x.Salary));

// Average performance per category
Dictionary<string, double> avgPerfByCategory = collection
    .AsQueryable()
    .GroupBy(x => x.Category)
    .ToDictionary(g => g.Key, g => g.Average(x => x.PerformanceScore));

// With filter applied first
Dictionary<string, int> activeCountByCategory = collection
    .AsQueryable()
    .Where(x => x.IsActive)
    .GroupBy(x => x.Category)
    .ToDictionary(g => g.Key, g => g.Count());
```

**Supported GroupBy aggregates:**
- `g.Key` - Group key
- `g.Count()` / `g.LongCount()` - Count per group
- `g.Sum(x => x.Column)` - Sum per group
- `g.Average(x => x.Column)` - Average per group
- `g.Min(x => x.Column)` - Minimum per group
- `g.Max(x => x.Column)` - Maximum per group

**Notes:**
- GroupBy key must be a simple column access (`x => x.Column`)
- Dictionary-encoded columns are fully supported for both keys and aggregates
- Aggregations must reference direct column properties
- `ToDictionary` key selector must be `g => g.Key`

### Compile-Time Diagnostics

the FrozenArrow.Analyzers package provides compile-time warnings and errors:

| Code | Severity | Description |
|------|----------|-------------|
| `ARROWQUERY001` | ⚠️ Warning | Using `Enumerable.Where()` instead of `Queryable.Where()` |
| `ARROWQUERY002` | ❌ Error | Unsupported LINQ method on ArrowQuery |
| `ARROWQUERY003` | ⚠️ Warning | Complex predicate may cause partial materialization |


| `ARROWQUERY004` | ❌ Error | Unsupported GroupBy projection |
| `ARROWQUERY007` | ⚠️ Warning | OR predicate reduces optimization |


Example diagnostic:

```csharp
// ⚠️ ARROWQUERY001: Using Enumerable.Where() bypasses optimization
var bad = collection.AsQueryable().ToList().Where(x => x.Age > 30);

// ✓ Correct: Use Queryable.Where() for optimized execution
var good = collection.AsQueryable().Where(x => x.Age > 30).ToList();
```

### Performance Example

```csharp
// Sample: 1 million records with 10 columns
using var collection = GenerateLargeDataset(1_000_000).ToFrozenArrow();

// Query: Find active engineers over 30
var results = collection
    .AsQueryable()
    .Where(x => x.Age > 30)           // Scans Age column only
    .Where(x => x.IsActive)            // Scans IsActive column only  
    .Where(x => x.Category == "Eng")   // Scans Category column only
    .ToList();

// Columns touched: 3 of 10
// Objects materialized: ~50,000 (matching rows only)
// Objects NOT created: 950,000!
```

## Diagnostic Messages

The source generator and analyzer produce helpful diagnostic messages:

### Source Generator Diagnostics

| Code | Severity | Description |
|------|----------|-------------|
| `ARROWCOL001` | Error | Type has `[ArrowRecord]` but no properties/fields marked with `[ArrowArray]` |
| `ARROWCOL002` | Error | Property/field has an unsupported type |
| `ARROWCOL003` | Error | Type is missing a public parameterless constructor |
| `ARROWCOL004` | Error | `[ArrowArray]` on a manual property (not an auto-property) |
| `ARROWCOL005` | Warning | Field has `[ArrowArray]` but no explicit `Name` specified |

### Query Analyzer Diagnostics

| Code | Severity | Description |
|------|----------|-------------|
| `ARROWQUERY001` | Warning | Using `Enumerable.Where()` on ArrowQuery bypasses optimization |
| `ARROWQUERY002` | Error | Unsupported LINQ method on ArrowQuery |
| `ARROWQUERY003` | Warning | Complex predicate may cause partial materialization |
| `ARROWQUERY004` | Error | Unsupported GroupBy projection |
| `ARROWQUERY005` | Warning | Mixing LINQ providers may cause unexpected materialization |
| `ARROWQUERY007` | Warning | OR predicate reduces optimization |


## Performance Characteristics

### Advantages
- **Memory Efficiency**: Significant compression for large datasets using Apache Arrow's columnar format
- **Multiple Enumerations**: Can enumerate the collection multiple times
- **Immutability**: Thread-safe for reading (data is frozen after creation)
- **Source-Generated**: Zero reflection at runtime for item creation (IL-emitted field accessors)
- **Efficient Serialization**: Arrow IPC format preserves columnar structure for fast I/O
- **Column-Level Filtering**: Query predicates evaluated on columns, not objects
- **Selective Materialization**: Only matching rows are converted to objects
- **Column-Level Aggregates**: Sum, Average, Min, Max computed on columns directly
- **Single-Pass Multi-Aggregates**: Compute multiple aggregates without multiple iterations
- **Dictionary-Encoded Support**: Full support for dictionary-encoded columns in GroupBy and aggregates
- **SIMD Acceleration**: AVX2/AVX-512 optimized bitmap operations, predicate evaluation, and aggregation

### SIMD Optimizations

FrozenArrow uses hardware-accelerated SIMD operations for maximum throughput:

| Component | Operation | Vectorization |
|-----------|-----------|---------------|
| **SelectionBitmap** | AND, OR, NOT, AndNot | AVX-512/AVX2/SSE2 (256-512 bits/instruction) |
| **SelectionBitmap** | CountSet | Hardware POPCNT (4 blocks/iteration) |
| **SelectionBitmap** | Any, All | SIMD short-circuit check |
| **Int32 Predicate** | Comparison (<, >, ==, etc.) | AVX2 (8 values/iteration) |
| **Double Predicate** | Comparison | AVX2 (4 values/iteration) |
| **Grouped Sum** | Index gathering + accumulation | Manual SIMD gather (64+ items/group) |
| **Grouped Min/Max** | Horizontal reduction | Vector256.Min/Max |
| **Single-Pass GroupBy** | Low-cardinality keys (≤256) | Array-based accumulators (no hash lookups) |

### Trade-offs
- **Enumeration Cost**: Items are reconstructed on-the-fly, which is slower than iterating in-memory objects
- **Not for Frequent Access**: Best suited for scenarios where data is enumerated infrequently but needs to be kept in memory
- **Construction Cost**: Initial creation requires copying all data into Arrow format
- **Query Limitations**: Complex predicates may require fallback to row-by-row evaluation



## Use Cases

FrozenArrow is ideal for:

- **Caching large datasets** that are infrequently accessed
- **In-memory analytics** where memory is constrained
- **Reference data** that needs to be kept in memory but rarely accessed
- **Historical data** that must be available but isn't frequently queried
- **Data persistence** with efficient columnar storage format
- **Cross-language interop** via Arrow IPC format (Python, Rust, Java, etc.)
- **OLAP-style queries** with efficient column-level filtering
- **Large dataset filtering** where only a subset of rows match criteria
- **Aggregate computations** over filtered data without full materialization
- **GroupBy analytics** with column-level aggregation


## Project Structure

```
FrozenArrow/
├── src/
│   ├── FrozenArrow/                  # Core library + Query engine
│   │   └── Query/                    # ArrowQuery LINQ implementation
│   ├── FrozenArrow.Generators/       # Source generator
│   └── FrozenArrow.Analyzers/        # Roslyn analyzer for query diagnostics
├── tests/
│   └── FrozenArrow.Tests/            # Unit tests
├── benchmarks/
│   ├── FrozenArrow.Benchmarks/       # Performance benchmarks
│   └── FrozenArrow.MemoryAnalysis/   # Memory footprint analysis
├── profiling/
│   └── FrozenArrow.Profiling/        # Query execution profiling tool
└── samples/
    └── FrozenArrow.Sample/           # Sample application
```

## Requirements

- .NET 10.0 or later
- Apache.Arrow NuGet package (automatically included)

## Performance Overview

FrozenArrow is designed for **memory efficiency**, not raw speed. Here's how it compares:

### Memory Footprint

| Scenario | List<T> | FrozenArrow | Savings |
|----------|---------|-------------|---------|
| Standard model (1M items) | 123 MB | 27 MB | **78%** |
| Wide model (200 cols, 1M items) | 1,786 MB | 985 MB | **45%** |

### Query Performance (1M items)

| Operation | List | FrozenArrow | DuckDB |
|-----------|------|-------------|--------|
| Filter + Count | 4.6 ms | 9.4 ms | **422 μs** |
| Sum (filtered) | 10.3 ms | 35.6 ms | **601 μs** |
| GroupBy + Count | 23.2 ms | **9.9 ms** | 4.5 ms |
| Any (short-circuit) | **13 ns** | 6.8 μs | 319 μs |

### When to Use Each Technology

| Use Case | Best Choice |
|----------|-------------|
| Aggregations at scale | **DuckDB** (10-50x faster) |
| Memory-constrained environments | **FrozenArrow** (45-78% smaller) |
| Short-circuit operations (Any/First) | **List<T>** (nanoseconds) |
| .NET-native LINQ API | **FrozenArrow** (no SQL) |
| Complex JOINs | **DuckDB** (not supported in FA) |

### Detailed Benchmarks

For comprehensive benchmark results, see:
- **[Performance Benchmarks](benchmarks/FrozenArrow.Benchmarks/README.md)** - Filter, Aggregation, GroupBy, Pagination, Serialization
- **[Memory Analysis](benchmarks/FrozenArrow.MemoryAnalysis/README.md)** - Static footprint, query overhead, wide model analysis
- **[Query Profiling](profiling/FrozenArrow.Profiling/README.md)** - Detailed query execution profiling and phase analysis

### Query Execution Profile (1M rows)

| Component | Time | Throughput |
|-----------|------|------------|
| Bitmap operations | 571 μs | 1,750 M rows/s |
| Simple aggregates | 765 μs | 1,308 M rows/s |
| Predicate evaluation | 4.4 ms | 227 M rows/s |
| Filter (3 predicates) | 4.6 ms | 217 M rows/s |
| Parallel speedup | 8.55x | on 24 cores |

### Running Benchmarks

```bash
# Run performance benchmarks
dotnet run -c Release --project benchmarks/FrozenArrow.Benchmarks -- --filter *Filter*

# Run memory analysis
dotnet run -c Release --project benchmarks/FrozenArrow.MemoryAnalysis

# Run query profiling (1M rows baseline)
dotnet run -c Release --project profiling/FrozenArrow.Profiling -- -s all -r 1000000 -v
```

## License

See LICENSE file for details.
