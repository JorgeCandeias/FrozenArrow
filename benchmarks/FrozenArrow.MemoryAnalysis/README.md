# FrozenArrow Memory Analysis

This directory contains memory footprint analysis comparing FrozenArrow against alternative technologies.

## Organization Principles

Memory analysis is organized **by model type**, not by technology. All competing technologies appear side-by-side in each analysis.

### Key Rules

1. **All technologies compete together** - Easy side-by-side comparison
2. **Consistent measurement approach** - Uses `Process.PrivateMemorySize64` for both managed and native memory
3. **Standard item counts** - Use 10K, 100K, 1M for standard model; 1M for wide model

## Analysis Files

| File | Purpose | Technologies |
|------|---------|--------------|
| `StandardModelAnalyzer.cs` | 7-column model memory footprint | List, FrozenArrow, DuckDB |
| `HeavyRecordMemoryAnalyzer.cs` | 200-column model memory footprint | List, FrozenArrow |
| `Shared/AnalysisHelpers.cs` | Common utilities and GC helpers | N/A |
| `Shared/AnalysisModels.cs` | Standard model class + factory | N/A |

## Data Models

### Standard Model (7 columns)
`MemoryAnalysisItem`:
- `Id` (int), `Name` (string), `Age` (int), `Salary` (decimal)
- `IsActive` (bool), `Category` (string), `Department` (string)

### Wide Model (200 columns)
`HeavyMemoryTestItem`:
- 10 string properties (low cardinality: 100 distinct values)
- 5 DateTime properties (high cardinality: unique timestamps)
- 62 int, 62 double, 61 decimal properties (sparse: ~10 active per row)

## Running Analysis

```bash
dotnet run -c Release --project benchmarks/FrozenArrow.MemoryAnalysis
```

## Measurement Methodology

We use `Process.PrivateMemorySize64` because it captures both managed heap AND native memory (Arrow buffers), providing an accurate "cost to hold this data" measurement.

## Latest Results

> **Environment**: Windows 11, .NET 10.0.2

### Standard Model (7 columns)

#### Static Memory Footprint

| Items | List | FrozenArrow | DuckDB | FA vs List | FA vs DuckDB |
|-------|------|-------------|--------|------------|--------------|
| 10,000 | 2.0 MB | 6.8 MB | 6.7 MB | 3.4x larger | same |
| 100,000 | 12.3 MB | 3.7 MB | 14.1 MB | **70% smaller** | **73% smaller** |
| 500,000 | 61.2 MB | 14.2 MB | 53.3 MB | **77% smaller** | **73% smaller** |
| 1,000,000 | 116.9 MB | 26.8 MB | 91.9 MB | **77% smaller** | **71% smaller** |

#### Query Memory Overhead (500K items)

| Query Type | List | FrozenArrow | DuckDB |
|------------|------|-------------|--------|
| Count (no materialization) | 0 B | 19.9 MB | 48 KB |
| Sum aggregation | 0 B | 18.8 MB | 860 KB |
| GroupBy + Sum | 21.9 MB | 474.4 MB | 11.5 MB |

### Wide Model (200 columns, 1M items)

#### Static Memory Footprint

| Storage | Memory | Savings vs List |
|---------|--------|-----------------|
| List<T> | 1,783 MB | — |
| FrozenArrow | 986 MB | **44.7%** |

#### Dictionary Encoding Impact

- **195 of 200 columns** automatically use dictionary encoding
- Low-cardinality strings (100 distinct values) compress extremely well
- Sparse numeric columns benefit from value deduplication

## Key Insights

### When FrozenArrow Excels

1. **Large datasets (100K+ items)** - 70-77% memory savings at scale
2. **Wide, sparse datasets** - 44.7% savings on 200-column records
3. **Low-cardinality strings** - Dictionary encoding provides massive savings
4. **Read-heavy workloads** - Static data held in memory benefits from columnar format

### When FrozenArrow Costs More

1. **Small datasets (<10K items)** - Columnar overhead exceeds benefits
2. **Query memory during execution** - Building bitmaps and projections requires working memory
3. **GroupBy operations** - Higher memory allocation during query execution

### Comparison Summary

| Scenario | Best Choice | Why |
|----------|-------------|-----|
| Large static datasets | **FrozenArrow** | 70-77% memory savings |
| Wide sparse tables | **FrozenArrow** | 45% memory savings |
| Small datasets | **List<T>** | Lower overhead |
| Complex aggregations | **DuckDB** | Lower query memory |
| Memory-constrained environments | **FrozenArrow** | Best compression |

## Adding a New Technology

1. Add measurement code to `StandardModelAnalyzer.cs` and/or `HeavyRecordMemoryAnalyzer.cs`
2. Follow the measurement protocol (GC ? baseline ? create ? GC ? measure)
3. Update this README with results

See the main [README.md](../../README.md) for complete guidance.
