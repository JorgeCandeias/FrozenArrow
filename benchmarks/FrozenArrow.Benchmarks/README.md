# FrozenArrow Benchmarks

This directory contains performance benchmarks comparing FrozenArrow against alternative technologies.

## Organization Principles

Benchmarks are organized **by operation type**, not by technology. All competing technologies (List, FrozenArrow, DuckDB) appear side-by-side in each benchmark file.

### Key Rules

1. **No `Baseline = true` markers** - Let results rank naturally by speed
2. **All technologies compete in the same file** - Easy comparison
3. **Consistent naming**: `{Technology}_{Operation}` (e.g., `List_Filter_Count`, `FrozenArrow_Filter_Count`)
4. **Consistent scale params** - Use `[Params(10_000, 100_000, 1_000_000)]` for all benchmarks
5. **ShortRunJob for all** - Fast iteration during development

## Benchmark Files

### User-Facing Benchmarks

| File | Purpose | Technologies |
|------|---------|--------------|
| `FilterBenchmarks.cs` | Where clauses at various selectivities | List, FrozenArrow, DuckDB |
| `AggregationBenchmarks.cs` | Sum, Average, Min, Max | List, FrozenArrow, DuckDB |
| `GroupByBenchmarks.cs` | Grouped aggregations | List, FrozenArrow, DuckDB |
| `PaginationBenchmarks.cs` | Take, Skip, First, Any | List, FrozenArrow, DuckDB |
| `SerializationSizeBenchmarks.cs` | Arrow IPC vs Protobuf (wide model) | Arrow, Protobuf |
| `WideRecordQueryBenchmarks.cs` | 200-column record queries | List, FrozenArrow |
| `FrozenArrowBenchmarks.cs` | Core construction/enumeration | List, FrozenArrow |

### Internal Benchmarks

| File | Purpose |
|------|---------|
| `Internals/SelectionBitmapBenchmarks.cs` | SIMD bitmap operations |
| `Internals/PredicateEvaluationBenchmarks.cs` | Column scan performance |

## Running Benchmarks

```bash
# List all benchmarks
dotnet run -c Release -- --list flat

# Run by operation type
dotnet run -c Release -- --filter *Filter*
dotnet run -c Release -- --filter *Aggregation*
dotnet run -c Release -- --filter *GroupBy*
dotnet run -c Release -- --filter *Pagination*
dotnet run -c Release -- --filter *Serialization*

# Run by technology
dotnet run -c Release -- --filter *DuckDB*
dotnet run -c Release -- --filter *List_*
dotnet run -c Release -- --filter *FrozenArrow_*
```

## Latest Results

> **Environment**: Windows 11, .NET 10.0.2, BenchmarkDotNet v0.14.0

### Filter Operations

#### Filter + Count (High Selectivity ~5%)

| Method | 10K | 100K | 1M |
|--------|-----|------|-----|
| **DuckDB** | 255 ?s | 284 ?s | 422 ?s |
| List | 21 ?s | 385 ?s | 4.6 ms |
| FrozenArrow | 83 ?s | 989 ?s | 9.4 ms |

#### Filter + Count (Low Selectivity ~70%)

| Method | 10K | 100K | 1M |
|--------|-----|------|-----|
| **DuckDB** | 250 ?s | 296 ?s | 428 ?s |
| List | 28 ?s | 430 ?s | 6.0 ms |
| FrozenArrow | 51 ?s | 654 ?s | 7.7 ms |

#### Filter + ToList (High Selectivity ~5%)

| Method | 10K | 100K | 1M |
|--------|-----|------|-----|
| **List** | 20 ?s | 365 ?s | 5.1 ms |
| FrozenArrow | 323 ?s | 5.3 ms | 54 ms |
| DuckDB | 870 ?s | 7.3 ms | 43 ms |

### Aggregation Operations (Filtered)

| Method | 10K | 100K | 1M |
|--------|-----|------|-----|
| **DuckDB Sum** | 245 ?s | 370 ?s | 601 ?s |
| **DuckDB Avg** | 237 ?s | 368 ?s | 616 ?s |
| **DuckDB Min** | 244 ?s | 356 ?s | 601 ?s |
| **DuckDB Max** | 239 ?s | 363 ?s | 632 ?s |
| List Sum | 47 ?s | 625 ?s | 10.3 ms |
| List Avg | 47 ?s | 474 ?s | 8.3 ms |
| List Min | 41 ?s | 1.6 ms | 11.0 ms |
| List Max | 41 ?s | 1.6 ms | 11.2 ms |
| FrozenArrow Sum | 244 ?s | 3.6 ms | 35.6 ms |
| FrozenArrow Avg | 182 ?s | 1.7 ms | 16.7 ms |
| FrozenArrow Min | 278 ?s | 3.3 ms | 30.4 ms |
| FrozenArrow Max | 256 ?s | 3.3 ms | 30.1 ms |

### GroupBy Operations

| Method | 10K | 100K | 1M |
|--------|-----|------|-----|
| **DuckDB Count** | 1.1 ms | 2.7 ms | 4.5 ms |
| **DuckDB Sum** | 1.2 ms | 3.4 ms | 5.1 ms |
| FrozenArrow Count | 78 ?s | 873 ?s | 9.9 ms |
| FrozenArrow Sum | 428 ?s | 4.9 ms | 50.7 ms |
| List Count | 159 ?s | 2.2 ms | 23.2 ms |
| List Sum | 165 ?s | 2.8 ms | 41.7 ms |

### Pagination Operations

| Method | 10K | 100K | 1M |
|--------|-----|------|-----|
| **List Any** | 13 ns | 13 ns | 13 ns |
| **List First** | 3 ns | 3 ns | 4 ns |
| **List Take** | 212 ns | 213 ns | 214 ns |
| **List Skip+Take** | 2.5 ?s | 2.5 ?s | 2.5 ?s |
| FrozenArrow Any | 1.4 ?s | 1.8 ?s | 6.8 ?s |
| FrozenArrow First | 1.3 ?s | 1.8 ?s | 6.8 ?s |
| FrozenArrow Take | 36 ?s | 636 ?s | 7.6 ms |
| FrozenArrow Skip+Take | 37 ?s | 633 ?s | 7.5 ms |
| DuckDB Any | 310 ?s | 318 ?s | 319 ?s |
| DuckDB First | 167 ?s | 169 ?s | 158 ?s |
| DuckDB Take | 241 ?s | 254 ?s | 250 ?s |
| DuckDB Skip+Take | 258 ?s | 251 ?s | 265 ?s |

### Serialization (Standard Model - 10 columns)

| Method | 10K | 100K | 1M |
|--------|-----|------|-----|
| **Arrow (No Compression)** | 359 ?s | 4.5 ms | 24 ms |
| Arrow + LZ4 | 545 ?s | 5.7 ms | 56 ms |
| Arrow + Zstd | 1.4 ms | 11.7 ms | 117 ms |
| Protobuf | 2.1 ms | 25.9 ms | 231 ms |

### Serialization (Wide Model - 200 columns)

| Method | 10K | 100K | 1M |
|--------|-----|------|-----|
| **Arrow (No Compression)** | 2.0 ms | 38 ms | 225 ms |
| Protobuf | 10.3 ms | 103 ms | 935 ms |
| Arrow + LZ4 | 11.0 ms | 107 ms | 1,232 ms |
| Arrow + Zstd | 19 ms | 228 ms | 2,351 ms |

## Key Insights

### DuckDB Dominates

- **Aggregations**: 10-50x faster than alternatives at 1M scale
- **Filtered counts**: Consistently fastest across all selectivities
- **GroupBy**: 5-10x faster than List or FrozenArrow

### List Wins Short-Circuit

- **Any/First**: Nearly instant (nanoseconds) when data matches early
- **Simple materialization**: Lowest overhead for returning objects

### FrozenArrow Sweet Spots

- **GroupBy + Count**: 2.4x faster than List (columnar counting)
- **Memory-constrained scenarios**: See Memory Analysis for savings
- **.NET-native API**: No SQL strings, pure LINQ

### Arrow Serialization

- **2.5x faster than Protobuf** for uncompressed writes
- **Zstd compression**: 62% smaller than Protobuf at 1M items
- Best for storage/archival where size matters

## When to Use Each

| Scenario | Best Choice | Why |
|----------|-------------|-----|
| Aggregations at scale | **DuckDB** | 10-50x faster |
| Short-circuit ops (Any/First) | **List<T>** | O(1) when data matches |
| Memory-constrained | **FrozenArrow** | 70-77% memory savings |
| .NET-native LINQ API | **FrozenArrow** | No SQL, pure C# |
| Complex JOINs | **DuckDB** | Not supported in FrozenArrow |
| Serialization speed | **Arrow** | 2.5x faster than Protobuf |
| Serialization size | **Arrow + Zstd** | 62% smaller than Protobuf |

## Adding a New Technology

1. Add setup/cleanup to each relevant benchmark file
2. Add benchmark methods with naming: `{NewTech}_{Operation}`
3. Add to same categories as existing methods
4. Update this README with results

See the main [README.md](../../README.md) for complete guidance.
