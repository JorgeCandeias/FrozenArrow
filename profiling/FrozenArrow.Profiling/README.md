# FrozenArrow Query Profiling

> **?? For AI-Assisted Development**: This tool is the **primary verification method** for all optimization work in FrozenArrow.  
> See "Usage for AI-Assisted Optimization" section below for the mandatory workflow.

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

### Stability Options

| Option | Description | Default |
|--------|-------------|---------|
| `--no-outlier-removal` | Disable IQR-based outlier removal | Enabled |
| `--gc-between-iterations` | Force GC between each iteration (slower but more stable) | Disabled |
| `--no-priority-boost` | Don't elevate process priority during measurement | Enabled |
| `--stability-threshold` | CV threshold for stability warning (0.0-1.0) | `0.15` |

## Measurement Stability Features

The profiler includes several features to reduce variance and improve reliability:

### 1. Outlier Removal (IQR Method)
Measurements affected by GC or OS interruptions are automatically detected and removed using the Interquartile Range method. Results show "X outlier(s) removed" when this occurs.

### 2. Stability Detection
Results are flagged with ? if the Coefficient of Variation exceeds the threshold (default 15%), indicating unreliable measurements.

### 3. Process Priority Elevation
The profiler automatically elevates process and thread priority during measurements to reduce interference from other processes.

### 4. GC Control
- Full GC is forced before warmup and before measurement
- Optional `--gc-between-iterations` for scenarios with high allocation variance

### Tips for Stable Measurements

1. **Use sufficient iterations**: `-i 10` or higher for stable median
2. **Use sufficient warmup**: `-w 3` or higher for JIT compilation
3. **Close other applications**: Especially resource-intensive ones
4. **Use GC between iterations**: `--gc-between-iterations` for allocation-heavy scenarios
5. **Run multiple times**: If results vary significantly, something is interfering
6. **Check stability warnings**: ? indicators mean results may not be reliable

## Available Scenarios

| Scenario | Description |
|----------|-------------|
| `filter` | Filter operations with varying selectivity |
| `aggregate` | Sum, Average, Min, Max aggregations |
| `sparseagg` | Aggregation with sparse selections (1%, 10%, 50%) |
| `groupby` | GroupBy with aggregations |
| `fused` | Fused filter+aggregate (single-pass) |
| `parallel` | Sequential vs parallel comparison |
| `bitmap` | SelectionBitmap operations |
| `predicate` | Predicate evaluation (SIMD vs scalar) |
| `enumeration` | Result materialization (ToList, foreach) |
| `shortcircuit` | Any/First with early-exit streaming evaluation |
| `all` | Run all scenarios |

---

## Baseline Results

> **Environment**: Windows 11, .NET 10.0, 24-core CPU, AVX2 enabled, AVX-512 disabled  
> **Dataset**: 1,000,000 rows, 8 columns (int, double, bool, long)  
> **Configuration**: Release build, 10 iterations, 3 warmup  
> **Last Updated**: 2025-01-27 (after predicate reordering optimization)

### Summary Table

| Scenario | Median (탎) | M rows/s | Allocated |
|----------|-------------|----------|-----------|
| **BitmapOperations** | 518 | 1,933 | 24 B |
| **Aggregate** | 3,182 | 314 | 39 KB |
| **SparseAggregation** | 28,102 | 36 | 199 KB |
| **PredicateEvaluation** | 11,228 | 89 | 45 KB |
| **FusedExecution** | 9,950 | 101 | 58 KB |
| **Filter** | 8,082 | 124 | 40 KB |
| **GroupBy** | 23,520 | 43 | 64 KB |
| **ParallelComparison** | 25,357 | 39 | 56 KB |
| **ShortCircuit** | 28,219 | 35 | 127 KB |
| **Enumeration** | 110,645 | 9 | 116 MB |

### Key Findings

#### 1. **Bitmap Operations are Extremely Fast**
- PopCount: **3.7 탎** for 1M bits (hardware POPCNT)
- Create: **5.7 탎** for 122 KB bitmap
- Iteration: **665 탎** to enumerate 667K set bits
- SIMD: AVX2 enabled, AVX-512 not available

#### 2. **Aggregates Use Block-Based SIMD**
- All four aggregate operations (Sum, Average, Min, Max) use block-based bitmap iteration
- Dense blocks (all 64 bits set) use vectorized sum/min/max
- Sparse blocks use TrailingZeroCount for efficient bit extraction
- Total aggregation over 1M rows: **788 ?s** (1.27 billion rows/second)

#### 3. **Predicate Evaluation Scales Linearly**
- 1M rows filtered in **5.4 ms** (184M rows/second)
- Int32 SIMD comparisons: 1.8 ms (8 values/AVX2 instruction)
- Boolean predicates: 1.2 ms (direct bitmap extraction)
- Multi-predicate: 2.2 ms (includes bitmap intersection)

#### 4. **Parallel Execution Shows Strong Speedup**
- Sequential execution: **17,965 ?s**
- Parallel execution: **2,885 ?s**
- **Speedup: 6.23x** on 24-core machine
- Parallel overhead justified above ~50K rows

#### 5. **Predicate Evaluation Performance Varies by Type**
| Predicate Type | Time (?s) | % of Total |
|----------------|-----------|------------|
| Double predicate (SIMD) | 354 | 6.5% |
| Bool predicate | 1,236 | 22.7% |
| Int32 predicate (SIMD) | 1,819 | 33.5% |
| Multi-predicate | 2,163 | 39.8% |

- Double predicates are fastest due to lower cardinality filtering
- Int32 comparisons benefit from AVX2 (8 values/iteration)
- Multi-predicate overhead comes from bitmap intersection

#### 6. **Enumeration is the Dominant Cost**
- ToList (534K items): **68 ms** (65% of enumeration time)
- Foreach (311K items): **49 ms** (47% of enumeration time)
- First (1 item): **1.9 ms** (short-circuit works well)
- **Memory**: 232 MB allocated for ToList (~434 bytes/item)

#### 7. **GroupBy Performance**
- 20 groups over 1M rows
- GroupBy + Count: **6.9 ms**
- GroupBy + Sum: **17.0 ms**
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
Sum             197         24.8%
Max             197         24.8%
Min             197         24.8%
Average         197         24.8%
```
- All aggregates use block-based bitmap iteration
- Dense blocks (all bits set) use SIMD vector operations
- Memory bandwidth limited, not compute limited
- No per-row object allocation

### Parallel Comparison Scenario
```
Phase           Time (탎)   % of Total
???????????????????????????????????????
Sequential      17,931      79.7%
Parallel         6,840      30.4%
```
- **6.23x speedup** from parallelization
- Parallel threshold is 10K rows by default (configurable)
- Chunk size is 16KB (optimized for L2 cache)

### Bitmap Operations Scenario
```
Phase           Time (탎)   % of Total
???????????????????????????????????????
ClearBits       817         143.8%*
IterateIndices  665         117.1%*
Create          5.7         1.0%
PopCount        3.7         0.6%
```
*Percentages exceed 100% because phases overlap differently than main measurement

- **ClearBits**: Simulates filter evaluation (clearing every 3rd bit)
- **IterateIndices**: Uses TrailingZeroCount for efficient bit scanning
- **PopCount**: Uses hardware POPCNT instruction
- **Create**: ArrayPool allocation + initial fill

### Sparse Aggregation Scenario

This scenario specifically tests the **block-based bitmap iteration optimization**, which excels when processing sparse selections after filtering.

```
Phase                Time (탎)   % of Total   Selection Rate
??????????????????????????????????????????????????????????????
Sum1PctFilter         8,459      29.5%        2.2% (22K rows)
Sum10PctFilter        8,591      29.9%        8.9% (89K rows)
Sum50PctFilter        9,187      32.0%        48.9% (489K rows)
Sum100PctNoFilter     2,817       9.8%        100% (1M rows)
```

**Key Insights:**
- **Sparse selections (1-10%)** are nearly as fast as dense (50%) due to block skipping
- **Block-based iteration** processes 64 bits at a time, skipping empty blocks entirely
- **No-filter baseline** (2.8ms) shows aggregation alone is very fast with SIMD dense block optimization
- **Predicate evaluation dominates** the filtered cases (~8-9ms overhead)

**How block-based iteration helps sparse selections:**
| Selection Rate | Empty Blocks | Block Skipping Benefit |
|----------------|--------------|------------------------|
| 1% | ~85% | High - most blocks skipped |
| 10% | ~50% | Moderate - half blocks skipped |
| 50% | ~5% | Low - few blocks skipped |
| 100% | 0% | None - all blocks processed (SIMD kicks in) |

**Use this scenario to measure:**
- Block iteration efficiency improvements
- Predicate evaluation optimizations
- Sparse access pattern performance

### Short-Circuit Scenario

This scenario tests **streaming predicate evaluation** for operations that can exit early (Any, First, FirstOrDefault).

```
Phase                    Time (탎)   % of Total   Description
??????????????????????????????????????????????????????????????????
AnyEarlyMatch             2,761       8.5%        Match at row 0-10
AnyRestrictiveMatch       2,730       8.4%        Match within ~100 rows
FirstEarlyMatch           2,730       8.4%        First() with early match
AnyMultiPredicate         2,804       8.6%        Multi-predicate, early match
AnyNoMatch               12,331      37.9%        Full scan, no match found
FirstOrDefaultNoMatch    12,391      38.1%        Full scan, returns default
```

**Key Insights:**
- **Early match is 4-5x faster** than no-match (2.7ms vs 12.3ms)
- **Streaming evaluation** avoids building full bitmap when match found early
- **Query parsing overhead** (~2.7ms) dominates for early matches
- **No-match case** scans all rows sequentially (no SIMD, ~3x slower than parallel)

**When short-circuit helps:**
| Query Pattern | Benefit | Recommendation |
|---------------|---------|----------------|
| `Any()` with likely matches | **4-5x faster** | ? Use streaming |
| `First()` with likely matches | **4-5x faster** | ? Use streaming |
| `Any()` expecting no matches | 3x slower | ?? Consider Count() > 0 |
| Large datasets, early matches | **10-100x faster** | ? Huge benefit |

**Use this scenario to measure:**
- Short-circuit effectiveness
- Query parsing overhead
- Streaming vs bitmap trade-offs

---

## Optimization Opportunities

Based on this baseline, the following optimizations would have the highest impact:

### High Impact
1. **Enumeration/Materialization** - Currently 104ms; consider batch materialization or object pooling
2. **Multi-predicate evaluation** - Short-circuit evaluation when bitmap chunk becomes zero

### Medium Impact
3. **GroupBy with high cardinality** - Currently uses dictionary; consider hash-based grouping
4. **Bitmap iteration** - Consider PEXT instruction for extracting set bit positions

### Recently Implemented ? (2025-01-27)
5. **Null bitmap batch processing** - AND null bitmap with selection in bulk using SIMD  
   ? See `docs/optimizations/null-bitmap-batch-processing.md`
6. **Dense block SIMD aggregation** - Use SIMD to sum 64 contiguous values when bitmap block is fully selected  
   ? See `docs/optimizations/vectorized-dense-block-aggregation.md`
7. **Lazy bitmap for short-circuit** - Stream predicate evaluation for Any/First operations  
   ? See `docs/optimizations/lazy-bitmap-short-circuit.md`

### Already Optimized ?
8. **Block-based aggregation** - Uses TrailingZeroCount for sparse blocks, SIMD for dense blocks
9. **Bitmap PopCount** - Uses hardware POPCNT
10. **Simple aggregates** - Near memory bandwidth limit
11. **Boolean predicates** - Uses direct bitmap extraction

---

## Usage for AI-Assisted Optimization

> **?? This is the MANDATORY process for all optimization work in FrozenArrow.**

The profiling tool provides fast, objective verification of optimizations during development. 
This complements BenchmarkDotNet (for final validation) and unit tests (for correctness).

### Standard Workflow

#### 1. Establish Baseline (Before Making Changes)

**Always capture baseline before any code changes:**

```bash
cd profiling/FrozenArrow.Profiling
dotnet run -c Release -- -s all -r 1000000 --save baseline-YYYY-MM-DD-optimization-name.json
```

**Example naming conventions:**
- `baseline-2024-01-15-zone-maps.json`
- `baseline-2024-01-20-null-bitmap-batch.json`
- `baseline-2024-01-25-predicate-reorder.json`

**Store in**: `profiling/FrozenArrow.Profiling/baselines/` directory

#### 2. Make Changes

- Implement the optimization
- Ensure `dotnet build` succeeds
- Run relevant unit tests: `dotnet test`
- Add inline comments explaining non-obvious performance tricks

#### 3. Compare Performance

**After implementing changes, compare against baseline:**

```bash
dotnet run -c Release -- -s all -r 1000000 -c baseline-YYYY-MM-DD-optimization-name.json
```

**The comparison output will show:**
- ? **Improvements** (green): Scenarios that got faster
- ?? **Regressions** (red): Scenarios that got slower  
- ?? **Neutral** (gray): Scenarios with <5% change

#### 4. Drill Down on Issues

**If any scenario regressed or didn't improve as expected:**

```bash
# Get detailed phase-level breakdown
dotnet run -c Release -- -s <scenario> -v

# Example: investigate why filter didn't improve
dotnet run -c Release -- -s filter -v
```

The `-v` flag shows which **specific phases** consumed time, helping identify:
- Which part of the optimization worked
- Which part didn't work as expected
- Where regressions were introduced

#### 5. Document Results

**Include performance numbers in:**
- **Commit message**: Key improvements and any regressions with justification
- **PR description**: Full comparison output from step 3
- **Technical docs**: Expected improvements and when they apply
- **Summary document**: High-level overview for stakeholders

---

### Interpreting Results

#### ? Successful Optimization
```
Scenario: Filter
  Baseline: 4,600 ?s
  Current:  2,100 ?s
  Change:   -54.3% (2.19x faster) ?
  
Allocated:
  Baseline: 34 KB
  Current:  34 KB
  Change:   +0.0% (same)
```

**Action**: Document this success! No regressions, significant improvement.

#### ?? Regression to Investigate
```
Scenario: Aggregate
  Baseline: 788 ?s
  Current:  985 ?s
  Change:   +25.0% (slower) ??
```

**Action**: Drill down with `-v` flag:
```bash
dotnet run -c Release -- -s aggregate -v
```

This shows phase breakdown to identify which part regressed.

#### ?? Acceptable Trade-off
```
Scenario: Filter
  Baseline: 4,600 ?s
  Current:  2,100 ?s
  Change:   -54.3% (2.19x faster) ?

Scenario: Enumeration
  Baseline: 104,388 ?s
  Current:  107,200 ?s
  Change:   +2.7% (slower) ??
```

**Rationale**: 54% improvement in common operation (filter) is worth 2.7% regression in rare operation (full enumeration).

**Action**: Document the trade-off in commit message and technical docs.

#### ?? Red Flag - Stop and Investigate
```
Scenario: Filter
  Baseline: 4,600 ?s
  Current:  4,550 ?s
  Change:   -1.1% (minimal improvement) ??
```

**Problem**: Expected 20-50% improvement, only saw 1.1%.

**Action**: 
1. Verify the optimization is actually being used (add logging/breakpoints)
2. Check if test data triggers the optimization (might need different query)
3. Profile with `-v` to see if optimization path is taken

---

### Required Scenarios by Optimization Type

Not all scenarios need to be tested for every change. Focus on relevant ones:

| Optimization Area | Required Scenarios | Optional Scenarios |
|-------------------|--------------------|--------------------|
| **Predicate/Filter** | `filter`, `predicate` | `bitmap`, `fused` |
| **Aggregation** | `aggregate`, `fused` | `filter`, `parallel`, `sparseagg` |
| **Sparse/Block Iteration** | `sparseagg`, `aggregate` | `filter`, `bitmap` |
| **GroupBy** | `groupby` | `aggregate` |
| **Parallelization** | `parallel`, `filter` | `all` |
| **Short-circuit (Any/First)** | `shortcircuit` | `filter`, `enumeration` |
| **General/Unknown** | `all` | - |
| **Bitmap Operations** | `bitmap`, `filter` | `predicate`, `sparseagg` |

**Example**: If optimizing SIMD predicates, run: `filter`, `predicate`, and optionally `bitmap`.

**Example**: If optimizing block-based iteration, run: `sparseagg`, `aggregate`, and optionally `bitmap`.

**Example**: If optimizing Any/First early-exit, run: `shortcircuit`, and optionally `filter`.

---

### Baseline Management

**Organize baselines by date and optimization:**

```
profiling/FrozenArrow.Profiling/baselines/
  ??? baseline-2024-01-15-zone-maps.json
  ??? baseline-2024-01-20-null-bitmap-batch.json
  ??? baseline-2024-01-25-predicate-reorder.json
  ??? baseline-2024-01-30-bloom-filters.json
  ??? baseline-latest.json  # symlink to most recent baseline
```

**Best practices:**
- Keep baselines in version control (they're small JSON files)
- Name by date + optimization for easy tracking
- Compare new work against relevant baseline (not just latest)
- Create new baseline after major changes

---

### Acceptance Criteria

Before merging an optimization, verify:

? **Target scenario improved** by >5% (preferably >20%)  
? **No unrelated scenarios regressed** by >5% without justification  
? **Memory allocation stayed same** or decreased (or increase justified)  
? **Phase breakdown shows** optimization in correct place  
? **Documentation includes** verified before/after numbers

### Red Flags ??

**Stop and investigate if you see:**

? **Target scenario <5% improvement**  
   ? Optimization may not be working or test data doesn't trigger it

? **Multiple unrelated scenarios regress >10%**  
   ? Likely introduced a bug or algorithmic issue

? **Memory allocation increases >50%**  
   ? Check for unexpected allocations, missing ArrayPool returns

? **Any scenario becomes >2x slower**  
   ? Critical regression, investigate immediately

? **Improvement in one phase, regression in another**  
   ? May have shifted bottleneck; use `-v` to understand

---

### Quick Command Reference

```bash
# List all available scenarios
dotnet run -c Release -- --list

# Quick single-scenario test (for rapid iteration)
dotnet run -c Release -- -s filter -r 1000000

# Full baseline capture (mandatory before changes)
dotnet run -c Release -- -s all -r 1000000 --save baseline.json

# Compare after changes (mandatory after changes)
dotnet run -c Release -- -s all -r 1000000 -c baseline.json

# Phase-level breakdown for investigation
dotnet run -c Release -- -s aggregate -v

# Different row counts for scaling analysis
dotnet run -c Release -- -s filter -r 10000    # Small dataset
dotnet run -c Release -- -s filter -r 1000000  # Large dataset

# JSON output for scripting/automation
dotnet run -c Release -- -s all -o json > results.json

# Disable parallel to isolate sequential performance
dotnet run -c Release -- -s filter --no-parallel
```

---

### Integration with BenchmarkDotNet

**Use profiling tool for:**
- ? Development-time iteration (fast feedback, <5 seconds)
- ? Catching regressions early
- ? Understanding phase-level bottlenecks
- ? Comparing before/after quickly

**Use BenchmarkDotNet for:**
- ? Final validation with statistical rigor
- ? Publishing results to community
- ? Cross-version comparisons
- ? Multiple dataset sizes and configurations

**Recommended workflow:**
1. **Profiling tool** - Iterate during development (run 20+ times)
2. **Unit tests** - Verify correctness
3. **Profiling tool** - Final verification before commit
4. **BenchmarkDotNet** - Validate before PR/release
5. **Profiling tool** - Regression testing in CI/CD

---

### CI/CD Integration (Future)

The profiling tool is designed for CI/CD integration:

```bash
# In CI pipeline, compare PR against master baseline
dotnet run -c Release -- -s all -r 1000000 -c baseline-master.json -o json > pr-results.json

# Parse results to fail PR if regression >10%
jq '.[] | select(.percentChange > 10.0)' pr-results.json
```

---

### Example: Zone Map Optimization Session

Here's a real example of how the profiling tool was used for the zone map optimization:

#### 1. Establish Baseline
```bash
$ dotnet run -c Release -- -s all -r 1000000 --save baseline-before-zonemaps.json

Scenario           Median (?s)   Allocated
????????????????????????????????????????????
Filter             4,600         34 KB
Aggregate          788           18 KB
# ... other scenarios
```

#### 2. Implement Zone Maps
- Added `ZoneMap.cs` with min/max per chunk
- Modified `ColumnPredicate` to test zone maps
- Updated `ParallelQueryExecutor` to skip chunks

#### 3. Compare Results
```bash
$ dotnet run -c Release -- -s all -r 1000000 -c baseline-before-zonemaps.json

Scenario: Filter
  Baseline: 4,600 ?s
  Current:  1,200 ?s      # ? 3.8x faster!
  Change:   -73.9%

Scenario: Aggregate  
  Baseline: 788 ?s
  Current:  792 ?s        # ?? Neutral (no regression)
  Change:   +0.5%
```

#### 4. Verify with Verbose
```bash
$ dotnet run -c Release -- -s filter -v

Phase Breakdown:
  MultiFilter      : 450 ?s   (was 2,120 ?s) ?
  HighSelectivity  : 380 ?s   (was 1,472 ?s) ?
  LowSelectivity   : 370 ?s   (was 962 ?s)   ??
```

**Analysis**: Zone maps helped selective queries (first two) significantly, as expected. 
Low selectivity queries can't skip much, but no regression either.

#### 5. Document
- Commit message: "Add zone map skip-scanning: 3.8x faster for selective filters"
- Created `docs/optimizations/ZoneMapOptimization.md`
- Added benchmarks in `benchmarks/FrozenArrow.Benchmarks/Internals/ZoneMapBenchmarks.cs`

---

### Troubleshooting

**Problem**: "Baseline file not found"
```bash
Error: Could not load baseline file: baseline.json
```
**Solution**: Check the file path, ensure you're in the correct directory, and the baseline file exists.

**Problem**: "No improvement shown, but optimization should help"
```bash
Scenario: Filter
  Change: +0.2% (no change)
```
**Possible causes:**
1. Test data doesn't trigger optimization (e.g., zone maps need selective predicates)
2. Optimization not being executed (add logging to verify)
3. Overhead of optimization equals the benefit (check with `-v`)

**Problem**: "Unexpected regression in unrelated scenario"
```bash
Scenario: Bitmap
  Change: +45.0% (slower) ??
```
**Action**: Use `-v` to identify which phase regressed, then investigate why.

---

### Tips for Effective Profiling

1. **Always warm up** - Default 2 warmup iterations are usually sufficient
2. **Use consistent dataset size** - 1M rows is standard for comparisons
3. **Run on same machine** - Performance varies across machines
4. **Close other apps** - Minimize background interference
5. **Multiple runs for outliers** - Increase iterations (`-i 10`) if results vary
6. **Save every baseline** - Disk space is cheap, recreating baselines is expensive
7. **Compare apples to apples** - Same row count, same scenario

---

## Summary for AI Assistants

When asked to optimize FrozenArrow:

1. ? **Always establish baseline first** - Never skip this step
2. ? **Run appropriate scenarios** - Not all scenarios needed for every change  
3. ? **Compare objectively** - Let numbers guide decisions, not intuition
4. ? **Drill down on surprises** - Use `-v` to understand unexpected results
5. ? **Document thoroughly** - Include profiling results in all optimization docs

**The profiling tool is your objective truth** - Trust it more than intuition! ??

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
