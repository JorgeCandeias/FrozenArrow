# ArrowCollection - Frozen Collection with Apache Arrow Compression

> **Trademark Notice**: "Apache Arrow" and "Arrow" are trademarks of [The Apache Software Foundation](https://www.apache.org/). ArrowCollection is an independent, community-driven project and is not affiliated with, endorsed by, or sponsored by The Apache Software Foundation. The name "ArrowCollection" is used purely to describe the library's functionality: providing .NET collections backed by the Apache Arrow columnar format.

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
- **Optimized LINQ Queries**: `IQueryable<T>` implementation with column-level predicate pushdown
- **Compile-Time Diagnostics**: Roslyn analyzer catches inefficient query patterns

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
- **Indexer access**: No direct index-based access to items (use LINQ `.ElementAt()` if needed)

## Optimized LINQ Queries with ArrowQuery

ArrowCollection includes a powerful query engine that enables **optimized LINQ queries** directly against the columnar Arrow data, without materializing objects until needed.

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
using ArrowCollection.Query;

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

**Supported GroupBy aggregates:**
- `g.Key` - Group key
- `g.Count()` / `g.LongCount()` - Count per group
- `g.Sum(x => x.Column)` - Sum per group
- `g.Average(x => x.Column)` - Average per group
- `g.Min(x => x.Column)` - Minimum per group
- `g.Max(x => x.Column)` - Maximum per group

**Current Limitations:**
- GroupBy key must be a simple column access (`x => x.Column`)
- Dictionary-encoded string columns are not yet supported for GroupBy keys
- Aggregations must reference direct column properties

### Compile-Time Diagnostics

The ArrowCollection.Analyzers package provides compile-time warnings and errors:

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
using var collection = GenerateLargeDataset(1_000_000).ToArrowCollection();

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

### Trade-offs
- **Enumeration Cost**: Items are reconstructed on-the-fly, which is slower than iterating in-memory objects
- **Not for Frequent Access**: Best suited for scenarios where data is enumerated infrequently but needs to be kept in memory
- **Construction Cost**: Initial creation requires copying all data into Arrow format
- **Query Limitations**: Complex predicates may require fallback to row-by-row evaluation
- **Dictionary-Encoded Columns**: Some operations don't yet support dictionary-encoded string columns

## Use Cases

ArrowCollection is ideal for:

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
ArrowCollection/
├── src/
│   ├── ArrowCollection/              # Core library + Query engine
│   │   └── Query/                    # ArrowQuery LINQ implementation
│   ├── ArrowCollection.Generators/   # Source generator
│   └── ArrowCollection.Analyzers/    # Roslyn analyzer for query diagnostics
├── tests/
│   └── ArrowCollection.Tests/        # Unit tests
├── benchmarks/
│   ├── ArrowCollection.Benchmarks/   # Performance benchmarks
│   └── ArrowCollection.MemoryAnalysis/ # Memory footprint analysis
└── samples/
    └── ArrowCollection.Sample/       # Sample application
```

## Requirements

- .NET 10.0 or later
- Apache.Arrow NuGet package (automatically included)

## Benchmarks

The following benchmarks were run on:
- **OS**: Windows 11
- **Runtime**: .NET 10.0.2
- **Benchmark.NET**: v0.14.0

### Memory Footprint Analysis

**Scenario**: 1 million records with 200 columns (sparse wide dataset)

| Storage | Memory Usage | Savings |
|---------|-------------|---------|
| `List<T>` | 1,784 MB | — |
| `ArrowCollection<T>` | 988 MB | **44.6%** |

**Key observations**:
- ArrowCollection achieves ~45% memory reduction on wide, sparse datasets
- Dictionary encoding automatically applied to 195 low-cardinality columns
- Memory savings scale with data redundancy and column count

### Core Performance (Construction & Enumeration)


| Operation | 10K Items | 100K Items | 1M Items |
|-----------|-----------|------------|----------|
| **List<T> Construction** | 3.8 μs | 328 μs | 3.5 ms |
| **ArrowCollection Construction** | 3.2 ms | 37.8 ms | 381 ms |
| **List<T> Enumeration** | 4.8 μs | 173 μs | 4.4 ms |
| **ArrowCollection Enumeration** | 1.5 ms | 15.2 ms | 163 ms |

**Key observations**:
- Construction is ~100x slower due to columnar conversion and compression
- Enumeration is ~37x slower due to on-the-fly object reconstruction
- This trade-off is intentional: ArrowCollection is optimized for memory, not speed

### ArrowQuery Performance (Wide Records - 200 Columns)

This benchmark demonstrates the power of column-level filtering on wide tables.

**Scenario**: Filter 10,000 records with 200 columns, ~1% selectivity (highly selective filter)

| Method | Time | Allocated | vs List |
|--------|------|-----------|---------|
| `List<T>.Where().ToList()` | 18 μs | 928 B | 1.0x (baseline) |
| `ArrowQuery.Where().ToList()` | 813 μs | 847 KB | 44x slower |
| `ArrowCollection.Where().ToList()` | 47,737 μs | 24 MB | **2,593x slower** |

**Key insight**: ArrowQuery is **57x faster** than naive ArrowCollection enumeration because:
- ArrowQuery scans only 1 column (the filter column)
- Only matching rows (~1%) are reconstructed
- ArrowCollection (Enumerable) reconstructs ALL 10,000 rows with ALL 200 columns

### ArrowQuery Performance (Count Operation)

**Scenario**: Count filtered records without materializing objects

| Method | 10K Items | Allocated |
|--------|-----------|-----------|
| `List<T>.Where().Count()` | 22 μs | 0 B |
| `ArrowQuery.Where().Count()` | 98 μs | 36 KB |
| `ArrowCollection.Where().Count()` | 47,282 μs | 24 MB |

**Key insight**: ArrowQuery Count is **482x faster** than naive enumeration because:
- Selection bitmap is built from column scan
- Count is computed from bitmap
- **Zero object reconstruction!**

### When to Use ArrowQuery

| Scenario | Best Approach | Why |
|----------|--------------|-----|
| Highly selective filter (<10% match) | ✅ ArrowQuery | Avoids reconstructing 90%+ of rows |
| Counting/Any/All | ✅ ArrowQuery | No reconstruction needed |
| Wide tables (many columns) | ✅ ArrowQuery | Reconstruction cost is high |
| Low selectivity (>90% match) | ⚠️ List<T> | Reconstruction overhead exceeds benefit |
| Frequent iteration | ⚠️ List<T> | ArrowCollection optimizes for memory, not speed |

### Running Benchmarks

```bash
# List all available benchmarks
dotnet run -c Release --project benchmarks/ArrowCollection.Benchmarks -- --list flat

# Run ArrowQuery benchmarks
dotnet run -c Release --project benchmarks/ArrowCollection.Benchmarks -- --filter *ArrowQuery*

# Run memory analysis
dotnet run -c Release --project benchmarks/ArrowCollection.MemoryAnalysis
```

## License

See LICENSE file for details.
