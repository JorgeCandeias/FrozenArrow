# FrozenArrow Query Profiling

This directory contains a profiling tool for diagnosing performance characteristics of ArrowQuery operations. It provides detailed timing breakdowns, phase analysis, and comparison capabilities.

## Purpose

The profiling tool helps identify:
- **Hotspots**: Which query operations consume the most time
- **Regressions**: Compare performance before/after code changes
- **Parallelization efficiency**: Measure speedup from parallel execution
- **Memory pressure**: Track allocations per operation

## Quick Start

```bash
# List available scenarios
dotnet run -c Release -- --list

# Run all scenarios with default settings
dotnet run -c Release -- -s all

# Run specific scenario with more data
dotnet run -c Release -- -s filter -r 1000000 -i 10

# Get detailed phase breakdown
dotnet run -c Release -- -s aggregate -v

# Save baseline for comparison
dotnet run -c Release -- -s all -r 1000000 --save baseline.json

# Compare against baseline after changes
dotnet run -c Release -- -s all -r 1000000 -c baseline.json
```

## Command-Line Options

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--scenario` | `-s` | Scenario to run | `all` |
| `--rows` | `-r` | Number of rows in dataset | `100,000` |
| `--iterations` | `-i` | Measured iterations | `5` |
| `--warmup` | `-w` | Warmup iterations | `2` |
| `--output` | `-o` | Format: table, json, csv, markdown | `table` |
| `--save` | | Save results to file | |
| `--compare` | `-c` | Compare with baseline file | |
| `--verbose` | `-v` | Show phase breakdown | `false` |
| `--no-parallel` | | Disable parallel execution | |
| `--list` | `-l` | List scenarios and exit | |
| `--help` | `-h` | Show help | |

## Available Scenarios

| Scenario | Description |
|----------|-------------|
| `filter` | Filter operations with varying selectivity |
| `aggregate` | Sum, Average, Min, Max aggregations |
| `groupby` | GroupBy with aggregations |
| `fused` | Fused filter+aggregate (single-pass) |
| `parallel` | Sequential vs parallel comparison |
| `bitmap` | SelectionBitmap operations |
| `predicate` | Predicate evaluation (SIMD vs scalar) |
| `enumeration` | Result materialization (ToList, foreach) |
| `all` | Run all scenarios |

---

## Baseline Results

> **Environment**: Windows 11, .NET 10.0, 24-core CPU, AVX2 enabled, AVX-512 disabled  
> **Dataset**: 1,000,000 rows, 8 columns (int, double, bool, long)  
> **Configuration**: Release build, 10 iterations, 2 warmup

### Summary Table

| Scenario | Median (?s) | M rows/s | Allocated |
|----------|-------------|----------|-----------|
| **BitmapOperations** | 571 | 1,750 | 584 B |
| **Aggregate** | 765 | 1,308 | 35 KB |
| **PredicateEvaluation** | 4,400 | 227 | 47 KB |
| **Filter** | 4,600 | 217 | 34 KB |
| **FusedExecution** | 11,945 | 84 | 31 KB |
| **GroupBy** | 16,662 | 60 | 85 KB |
| **ParallelComparison** | 19,272 | 52 | 27 KB |
| **Enumeration** | 105,342 | 9 | 232 MB |

### Key Findings

#### 1. **Bitmap Operations are Extremely Fast**
- PopCount: **3.7 ?s** for 1M bits (hardware POPCNT)
- Create: **6.4 ?s** for 122 KB bitmap
- Iteration: **815 ?s** to enumerate 667K set bits
- SIMD: AVX2 enabled, AVX-512 not available

#### 2. **Aggregates are Highly Optimized**
- All four aggregate operations (Sum, Average, Min, Max) complete in ~200 ?s each
- Near-identical performance indicates memory bandwidth is the bottleneck, not computation
- Total aggregation over 1M rows: **765 ?s** (1.3 billion rows/second effective)

#### 3. **Predicate Evaluation Scales Linearly**
- 1M rows filtered in **4.4 ms** (227M rows/second)
- Int32 SIMD comparisons: 1.4 ms (8 values/AVX2 instruction)
- Boolean predicates: 0.95 ms (direct bitmap extraction)
- Multi-predicate: 1.9 ms (includes bitmap intersection)

#### 4. **Parallel Execution Shows Strong Speedup**
- Sequential execution: **17,121 ?s**
- Parallel execution: **2,001 ?s**
- **Speedup: 8.55x** on 24-core machine
- Parallel overhead justified above ~50K rows

#### 5. **Predicate Evaluation Performance Varies by Type**
| Predicate Type | Time (?s) | % of Total |
|----------------|-----------|------------|
| Double predicate (SIMD) | 226 | 5.1% |
| Bool predicate | 952 | 21.6% |
| Int32 predicate (SIMD) | 1,376 | 31.3% |
| Multi-predicate | 1,922 | 43.7% |

- Double predicates are fastest due to lower cardinality filtering
- Int32 comparisons benefit from AVX2 (8 values/iteration)
- Multi-predicate overhead comes from bitmap intersection

#### 6. **Enumeration is the Dominant Cost**
- ToList (534K items): **67 ms** (65% of enumeration time)
- Foreach (311K items): **49 ms** (47% of enumeration time)
- First (1 item): **1.6 ms** (short-circuit works well)
- **Memory**: 232 MB allocated for ToList (~434 bytes/item)

#### 7. **GroupBy Performance**
- 20 groups over 1M rows
- GroupBy + Count: **7.3 ms**
- GroupBy + Sum: **17.1 ms**
- Single-pass dictionary-based aggregation for low-cardinality keys (?256)

---

## Phase Breakdown Details

### Filter Scenario
```
Phase                    Time (?s)   % of Total
?????????????????????????????????????????????????
MultiFilter              2,120       46.1%
HighSelectivity          1,472       32.0%
LowSelectivity             962       20.9%
```
- **MultiFilter** (Age > 30 && IsActive && Salary > 50000): Evaluates 3 predicates, intersects bitmaps
- **HighSelectivity** (Age > 55, ~20% match): Fast due to low result count
- **LowSelectivity** (IsActive, ~70% match): Fastest due to simple boolean check

### Aggregate Scenario
```
Phase           Time (?s)   % of Total
???????????????????????????????????????
Sum             212         26.9%
Max             208         26.3%
Min             199         25.2%
Average         188         23.8%
```
- All aggregates are within ~12% of each other
- Memory bandwidth limited, not compute limited
- No per-row object allocation

### Parallel Comparison Scenario
```
Phase           Time (?s)   % of Total
???????????????????????????????????????
Sequential      17,055      88.3%
Parallel         2,148      11.1%
```
- **7.94x speedup** from parallelization (measured), **8.55x** (metadata)
- Parallel threshold is 10K rows by default (configurable)
- Chunk size is 16KB (optimized for L2 cache)

### Bitmap Operations Scenario
```
Phase           Time (?s)   % of Total
???????????????????????????????????????
ClearBits       820         143.7%*
IterateIndices  815         143.0%*
Create          6.4         1.1%
PopCount        3.7         0.6%
```
*Percentages exceed 100% because phases overlap differently than main measurement

- **ClearBits**: Simulates filter evaluation (clearing every 3rd bit)
- **IterateIndices**: Uses TrailingZeroCount for efficient bit scanning
- **PopCount**: Uses hardware POPCNT instruction
- **Create**: ArrayPool allocation + initial fill

---

## Optimization Opportunities

Based on this baseline, the following optimizations would have the highest impact:

### High Impact
1. **Enumeration/Materialization** - Currently 105ms; consider batch materialization or object pooling
2. **Multi-predicate evaluation** - Short-circuit evaluation when bitmap chunk becomes zero
3. **GroupBy with Sum** - 17ms is slower than expected; investigate value accessor overhead

### Medium Impact
4. **GroupBy with high cardinality** - Currently uses dictionary; consider hash-based grouping
5. **Bitmap iteration** - Consider PEXT instruction for extracting set bit positions
6. **Null bitmap pre-intersection** - AND null bitmap with selection before aggregation

### Low Impact (Already Optimized)
7. Bitmap PopCount - Already uses hardware POPCNT
8. Simple aggregates - Already near memory bandwidth limit
9. Boolean predicates - Already using direct bitmap extraction

---

## Usage for AI-Assisted Optimization

When using this tool for AI-assisted optimization:

1. **Establish baseline**: `dotnet run -c Release -- -s all -r 1000000 --save baseline.json`
2. **Make changes** to the query engine
3. **Compare**: `dotnet run -c Release -- -s all -r 1000000 -c baseline.json`
4. **Drill down**: `dotnet run -c Release -- -s <scenario> -v` for specific phase analysis

The JSON output format is designed for easy parsing:
```bash
dotnet run -c Release -- -s filter -o json | jq '.[] | {scenario: .scenarioName, median: .medianMicroseconds}'
```

---

## Test Data Model

The profiling scenarios use `ProfilingRecord` with 8 columns:

| Column | Type | Distribution |
|--------|------|--------------|
| Id | int | Sequential 0..N |
| Age | int | Uniform 20-64 |
| DepartmentId | int | Uniform 0-19 (20 groups) |
| Salary | double | Uniform 30K-200K |
| PerformanceScore | double | Uniform 0-5 |
| IsActive | bool | 70% true |
| IsManager | bool | 15% true |
| TenureDays | long | Uniform 0-3650 |

This model provides:
- Mix of data types (int, double, bool, long)
- Varying selectivities for filter testing
- Low cardinality for GroupBy testing (20 departments)
- No string columns (to focus on numeric performance)
