# FrozenArrow Memory Analysis

This directory contains memory footprint analysis comparing FrozenArrow against alternative technologies.

## Organization Principles

Memory analysis is organized **by analysis type**, not by technology. All competing technologies (List, FrozenArrow, DuckDB) appear side-by-side in each analysis.

### Key Rules

1. **All technologies compete together** - Easy side-by-side comparison
2. **Consistent measurement approach** - Uses `Process.PrivateMemorySize64` for both managed and native memory
3. **Multiple scenarios** - Standard model (10 columns) and wide model (200 columns)
4. **Clear output format** - Tabular results with ratios

## Analysis Files

| File | Purpose | Technologies |
|------|---------|--------------|
| `StandardModelAnalyzer.cs` | 7-column model memory footprint | List, FrozenArrow, DuckDB |
| `HeavyRecordMemoryAnalyzer.cs` | 200-column model memory footprint | List, FrozenArrow |
| `Shared/AnalysisHelpers.cs` | Common utilities and GC helpers | N/A |
| `Shared/AnalysisModels.cs` | Standard model class + factory | N/A |

## Data Models

### Standard Model (7 columns)
`MemoryAnalysisItem` - Used for most analysis:
- `Id` (int), `Name` (string), `Age` (int), `Salary` (decimal)
- `IsActive` (bool), `Category` (string), `Department` (string)

### Wide Model (200 columns)
`HeavyBenchmarkItem` - Used for wide record analysis:
- 10 string properties (low cardinality)
- 5 DateTime properties (high cardinality)
- 62 int, 62 double, 61 decimal properties (sparse)

## Running Analysis

```bash
# Run all analysis
dotnet run -c Release

# The output includes:
# 1. Static memory footprint comparison
# 2. String cardinality impact analysis
# 3. Wide record memory analysis
```

## Measurement Methodology

### Process Memory Measurement

We use `Process.PrivateMemorySize64` because:
- Captures both managed heap AND native memory (Arrow buffers)
- Provides accurate "cost to hold this data" measurement
- More realistic than allocation-only measurements

### Measurement Protocol

1. Force full GC with compaction
2. Record baseline memory
3. Create data structure
4. Force full GC again
5. Measure memory delta
6. Clean up and repeat for next technology

## Latest Results

> **Note**: Results from Windows 11, .NET 10.0.2

### Static Memory Footprint (Standard Model)

| Items | List | FrozenArrow | DuckDB | FA vs List | FA vs DuckDB |
|-------|------|-------------|--------|------------|--------------|
| 10,000 | 4.0 MB | 10.7 MB | 6.2 MB | 2.7x larger | 1.7x larger |
| 100,000 | 14.4 MB | 19.1 MB | 34.0 MB | 1.3x larger | **56% smaller** |
| 500,000 | 26.4 MB | 180.6 MB | 92.3 MB | 6.8x larger | 2.0x larger |
| 1,000,000 | 18.6 MB | 272.4 MB | 115.8 MB | 14.6x larger | 2.4x larger |

### Wide Record Memory Footprint (200 columns, 1M items)

| Storage | Memory Usage | Savings vs List |
|---------|-------------|-----------------|
| List<T> | 1,782 MB | — |
| FrozenArrow<T> | 993 MB | **44.3%** |

### Key Insights

- **FrozenArrow excels on wide, sparse datasets** (44% savings at 200 columns)
- **Memory characteristics vary with data shape** - narrow vs wide, dense vs sparse
- **Dictionary encoding** automatically applied to low-cardinality string columns
- **DuckDB has different memory profile** - query execution buffers vs static storage

## Adding a New Technology

When adding a new technology to compare:

1. Add measurement code to both `StandardModelAnalyzer.cs` and `WideModelAnalyzer.cs`
2. Follow the measurement protocol (GC, baseline, create, GC, measure)
3. Include in the comparison table output
4. Update this README with the new technology

See the main [README.md](../../README.md) for detailed analysis and guidance on when to use each technology.
