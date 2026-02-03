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

| Method | 100K items | 1M items |
|--------|-----------|----------|
| **DuckDB** | 284 ?s | 422 ?s |
| List | 385 ?s | 4.6 ms |
| FrozenArrow | 989 ?s | 9.4 ms |

#### Filter + Count (Low Selectivity ~70%)

| Method | 100K items | 1M items |
|--------|-----------|----------|
| **DuckDB** | 294 ?s | 424 ?s |
| List | 430 ?s | 6.0 ms |
| FrozenArrow | 654 ?s | 7.7 ms |

#### Filter + ToList (High Selectivity ~5%)

| Method | 100K items | 1M items |
|--------|-----------|----------|
| **List** | 365 ?s | 5.1 ms |
| FrozenArrow | 5.3 ms | 53.5 ms |
| DuckDB | 7.3 ms | 43.0 ms |

### Aggregation Operations (Filtered)

#### 1M Items

| Method | Sum | Average | Min | Max |
|--------|-----|---------|-----|-----|
| **DuckDB** | 597 ?s | 603 ?s | 611 ?s | 626 ?s |
| List | 10.2 ms | 8.2 ms | 10.9 ms | 11.2 ms |
| FrozenArrow | 35.7 ms | 19.2 ms | 30.4 ms | 30.1 ms |

### GroupBy Operations

#### 1M Items

| Method | Count | Sum | Average | Multi-Agg |
|--------|-------|-----|---------|-----------|
| **DuckDB** | 4.5 ms | 5.2 ms | 4.7 ms | 5.2 ms |
| FrozenArrow | 9.9 ms | 49.0 ms | 47.3 ms | 61.8 ms |
| List | 23.3 ms | 41.2 ms | 47.7 ms | 49.1 ms |

### Pagination Operations

#### 1M Items

| Method | Any | First | Take(100) | Skip+Take |
|--------|-----|-------|-----------|-----------|
| **List** | 13 ns | 3 ns | 212 ns | 2.5 ?s |
| FrozenArrow | 6.8 ?s | 6.9 ?s | 7.6 ms | 7.5 ms |
| DuckDB | 328 ?s | 168 ?s | 247 ?s | 246 ?s |

### Serialization (Wide Model - 200 columns)

#### 100K Items

| Method | Time | Allocated |
|--------|------|-----------|
| **Arrow (No Compression)** | 38 ms | 238 MB |
| Protobuf | 104 ms | 151 MB |
| Arrow + LZ4 | 105 ms | 90 MB |
| Arrow + Zstd | 227 ms | 64 MB |

#### 1M Items

| Method | Time | Allocated |
|--------|------|-----------|
| **Arrow (No Compression)** | 264 ms | 2,657 MB |
| Protobuf | 922 ms | 1,206 MB |
| Arrow + LZ4 | 1,250 ms | 663 MB |
| Arrow + Zstd | 2,340 ms | 460 MB |

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
