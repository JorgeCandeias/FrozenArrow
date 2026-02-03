# AI Assistant Guidelines for FrozenArrow

## Project Context

FrozenArrow is a high-performance, columnar data structure library using Apache Arrow for .NET. 
The project focuses on:
- **Query performance**: SIMD vectorization, parallel execution, zero-copy operations
- **Memory efficiency**: ArrayPool, stack allocation, minimal GC pressure
- **Analytical workloads**: Column-oriented processing, predicate pushdown, aggregation

**Target Frameworks**: .NET Standard 2.0 (for library compatibility), .NET 10 (for performance features)

---

## Core Development Principles

### Performance First
- Every change should consider: CPU cycles, memory bandwidth, cache locality
- Use SIMD when beneficial (AVX2/AVX-512)
- Avoid allocations in hot paths (use `ArrayPool`, `stackalloc`, `Span<T>`)
- Measure before and after - intuition is often wrong

### Zero Breaking Changes
- Optimizations should be transparent (no API changes when possible)
- Backward compatibility is critical
- New features use opt-in patterns

### Documentation is Code
Every optimization requires:
1. **Implementation** - Well-commented with algorithmic complexity
2. **Benchmarks** - Demonstrate improvement with BenchmarkDotNet
3. **Profiling results** - Verified with profiling tool (see below)
4. **Technical docs** - Explain technique, when it helps, trade-offs
5. **Summary** - High-level overview for stakeholders

---

## Creativity and Innovation

### Thinking Outside the Box

**This project welcomes radical ideas and unconventional approaches.**

The guidelines in this document focus on *verification* (ensuring changes work), not on *limiting what you try*. As a frontier AI model, your ability to make novel connections and challenge assumptions is one of your greatest strengths.

**When proposing optimizations, explore unconventional ideas:**

? **Reverse the problem:**
- Instead of "how to filter faster," ask "can we avoid filtering?"
- Instead of "optimize this loop," ask "can we eliminate the loop?"
- Instead of "speed up allocation," ask "can we avoid allocating?"

? **Borrow from other domains:**
- Graphics programming (texture compression, Z-buffer tricks)
- Database systems (learned indices, adaptive radix trees)
- Compiler optimization (loop fusion, strength reduction)
- Machine learning (learned query optimization)
- Hardware design (prefetching, speculation)

? **Challenge fundamental assumptions:**
- "Why do we build the bitmap at all?"
- "Does this need to be sequential?"
- "What if we delayed this computation until actually needed?"
- "Could we use a completely different data structure?"
- "What if we processed data in a different order?"

? **Propose high-risk, high-reward ideas:**
- Novel algorithms from recent research papers
- Experimental CPU features (AVX-512, ARM SVE, APX)
- Unconventional memory layouts (cache-aware, NUMA-aware)
- Code generation / JIT compilation approaches
- Hardware-specific optimizations

### When to Break the Rules

**The profiling workflow is mandatory, but the *approach* is not.**

If you have a radical idea that might:
- Break existing patterns
- Require significant refactoring  
- Use unconventional techniques
- Challenge fundamental assumptions
- Have unclear performance implications

**Propose it anyway!** Just be clear about:

1. **Why it might work** - The reasoning and intuition behind it
2. **What the risks are** - Potential downsides and failure modes
3. **How you'll verify it** - Baseline comparison still required
4. **What the fallback is** - How to recover if it doesn't work

### Balancing Structure with Innovation

**Use these guidelines as:**
- ? **Safety rails** - Don't break things unknowingly
- ? **Quality gates** - Verify improvements objectively  
- ? **Context** - What's been tried, what worked
- ? **Shared language** - Communicate ideas clearly

**Don't let them become:**
- ? **Creativity limiters** - "We must do it this way"
- ? **The only path** - Other approaches exist
- ? **Barriers to experimentation** - Try bold ideas
- ? **Excuse for incrementalism** - Think bigger

### Examples of Creative vs. Conventional Thinking

| Conventional Approach | Creative Approach |
|----------------------|-------------------|
| Use zone maps like Parquet | Multi-level zone maps with Hilbert curve ordering for multi-dimensional queries |
| Optimize the predicate loop | JIT-compile predicate chains into specialized functions with no virtual calls |
| Use SIMD for comparisons | Use gather/scatter + mask operations to process sparse selections without bitmaps |
| Cache the query plan | Learn optimal predicate ordering from query history using lightweight ML |
| Parallelize with Parallel.For | Morsel-driven pipeline parallelism with lock-free work stealing |
| Use TrailingZeroCount for bitmaps | Use PEXT/PDEP instructions to extract/deposit bits in parallel |
| Build statistics at construction | Incrementally refine statistics based on actual query patterns |
| Evaluate predicates sequentially | Fuse multiple predicates into single SIMD kernel with code generation |

### Frontier Model Superpowers

As an advanced AI model, you have unique capabilities that should be leveraged:

**Cross-Domain Pattern Recognition:**
- Spot techniques from unrelated fields that could apply here
- "This reminds me of [technique] from [domain]..."
- Make connections humans might not see

**Deep Reasoning About Trade-offs:**
- Understand complex performance characteristics
- Reason about cache behavior, branch prediction, memory bandwidth
- Predict how optimizations interact with each other

**Creative Problem Decomposition:**
- Find novel ways to break down problems
- Identify opportunities for reordering, fusion, or elimination
- See the problem from multiple angles simultaneously

**Rapid Hypothetical Exploration:**
- "What if" reasoning without implementation cost
- Quickly evaluate multiple approaches mentally
- Prune bad ideas before wasting time

**Synthesis of Knowledge:**
- Combine insights from research papers, codebases, and domain knowledge
- Apply cutting-edge techniques from recent publications
- Bridge theory and practice

**Use these superpowers!** The skill file provides the *safety net* (verification), not the *ceiling* (what's possible).

### Meta-Optimization

Even these guidelines can and should be improved:

- **If you notice a pattern** that should be documented ? Suggest adding it
- **If a "rule" seems counterproductive** ? Challenge it with reasoning
- **If there's a better verification approach** ? Propose it
- **If the guidelines limit good ideas** ? Point it out

**This is a living document, not dogma.** Help make it better.

### The Golden Rule

**Creativity in approach + Rigor in verification = Breakthrough optimizations**

- Think radically about *what* to try
- Be disciplined about *how* to verify
- Document *why* it worked (or didn't)

---

## Performance Verification Process

**MANDATORY for all optimization work:**

### 1. Establish Baseline (Before Any Changes)

```bash
cd profiling/FrozenArrow.Profiling
dotnet run -c Release -- -s all -r 1000000 --save baseline-YYYY-MM-DD-{optimization-name}.json
```

**Example naming:**
- `baseline-2024-01-15-zone-maps.json`
- `baseline-2024-01-20-null-bitmap-batch.json`
- `baseline-2024-01-25-predicate-reorder.json`

**Store baselines in**: `profiling/FrozenArrow.Profiling/baselines/`

### 2. Implement Changes

- Make the optimization
- Ensure `dotnet build` succeeds
- Run relevant unit tests
- Add inline comments explaining non-obvious performance tricks

### 3. Verify Improvement (After Changes)

```bash
cd profiling/FrozenArrow.Profiling
dotnet run -c Release -- -s all -r 1000000 -c baseline-YYYY-MM-DD-{optimization-name}.json
```

**This will show:**
- ? **Improvements** (green): Scenarios that got faster
- ?? **Regressions** (red): Scenarios that got slower
- ?? **Neutral** (gray): Scenarios with <5% change

### 4. Drill Down (If Needed)

```bash
# Get phase-level breakdown for specific scenario
dotnet run -c Release -- -s {scenario} -v

# Example: investigate filter regression
dotnet run -c Release -- -s filter -v
```

### 5. Document Results

Include performance numbers in:
- **Commit message**: Key improvements/regressions
- **PR description**: Full comparison output
- **Technical docs**: Expected improvements and when they apply
- **Summary document**: High-level overview

### Required Scenarios by Optimization Type

| Optimization Type | Required Scenarios to Test |
|-------------------|----------------------------|
| Predicate/Filter optimization | `filter`, `predicate`, `bitmap` |
| Aggregation optimization | `aggregate`, `fused`, `sparseagg` |
| Sparse/Block iteration optimization | `sparseagg`, `aggregate`, `bitmap` |
| GroupBy optimization | `groupby` |
| Parallelization changes | `parallel`, `all` |
| General/Unknown impact | `all` |

### Acceptance Criteria

? **Target scenario shows improvement** (>5% speedup)  
? **No unrelated scenarios regress** (>5% slowdown) without justification  
? **Memory allocation doesn't increase significantly** (>20%) without reason  
? **Improvement is documented** with before/after numbers  
? **Baseline saved** for future comparisons  
? **Profiling README updated** if new scenario added or significant results changed

### Red Flags ??

**Stop and investigate if:**
- ? Target scenario shows <5% improvement (optimization may not be working)
- ? Multiple unrelated scenarios regress >10%
- ? Memory allocation increases >50% without clear justification
- ? Any scenario becomes >2x slower

---

## Standard Interaction Pattern

When a user requests optimization work:

### Step 1: Analysis
```
# Explore codebase structure
- get_projects_in_solution
- get_files_in_project
- code_search for relevant patterns
```

### Step 2: Baseline Capture
```
AI: "First, let me establish a performance baseline..."

cd profiling/FrozenArrow.Profiling
dotnet run -c Release -- -s all -r 1000000 --save baseline-{optimization}.json
```

### Step 3: Proposal
```
AI: "I've identified the following optimization opportunities:

1. Zone Maps (Priority 1) - High impact, medium effort
   Expected: 10-50x for sorted data with selective predicates
   
2. Null Bitmap Batch Processing (Priority 2) - High impact, low effort
   Expected: 5-10% improvement on nullable columns
   
3. Predicate Reordering (Priority 3) - Medium impact, low effort
   Expected: 10-20% for multi-predicate queries

Which would you like me to implement?"
```

### Step 4: Implementation
- Implement incrementally
- Build and test frequently
- Add comprehensive comments
- Follow existing code style

### Step 5: Verification
```
AI: "Now let's verify the optimization worked..."

dotnet run -c Release -- -s all -r 1000000 -c baseline-{optimization}.json
```

### Step 6: Drill-down (if needed)
```
AI: "I see a regression in the aggregate scenario. Let me investigate..."

dotnet run -c Release -- -s aggregate -v
```

### Step 7: Documentation
- Create implementation summary
- Document technique and when it applies
- Include verified performance numbers
- Add benchmarks for community sharing
- **Update `profiling/FrozenArrow.Profiling/README.md`** with:
  - New scenario if one was added (update "Available Scenarios" table)
  - Updated baseline results if significant changes occurred
  - New phase breakdown details for affected scenarios

### Step 8: Baseline Update
```
# Save new baseline for future work
dotnet run -c Release -- -s all -r 1000000 --save baseline-after-{optimization}.json
```

### Step 9: Add New Profiling Scenario (if needed)

When an optimization targets a specific pattern not well-covered by existing scenarios:

1. **Create new scenario** in `profiling/FrozenArrow.Profiling/Scenarios/`
2. **Register in `Program.cs`**:
   - Add to `ListScenarios()` help text
   - Add to scenario switch statement  
   - Add to `GetAllScenarios()` list
3. **Update README.md**:
   - Add to "Available Scenarios" table
   - Add to "Required Scenarios by Optimization Type" table
   - Add phase breakdown section with baseline results
4. **Save baseline** for future comparisons:
   ```bash
   dotnet run -c Release -- -s {new-scenario} -r 1000000 --save baselines/baseline-{scenario-name}.json
   ```

---

## Code Quality Standards

### .NET Version Features
- **Prefer .NET 10 features** for performance code (main library)
- **Maintain .NET Standard 2.0 compatibility** where required
- Use modern C# (C# 14.0): `ref struct`, `Span<T>`, pattern matching

### Performance Patterns

#### Hot Path Optimization
```csharp
// Mark critical paths for inlining
[MethodImpl(MethodImplOptions.AggressiveInlining)]
public void HotPath() { }

// Use ref for zero-copy
public void ProcessData(ref SelectionBitmap bitmap) { }

// Stackalloc for small temporary buffers
Span<int> buffer = stackalloc int[64];
```

#### Memory Management
```csharp
// Use ArrayPool for temporary buffers
var buffer = ArrayPool<int>.Shared.Rent(size);
try 
{
    // Use buffer
}
finally 
{
    ArrayPool<int>.Shared.Return(buffer);
}

// Or use 'using' with custom struct
using var bitmap = SelectionBitmap.Create(count);
```

#### SIMD Operations
```csharp
// Check hardware capabilities
if (Vector256.IsHardwareAccelerated)
{
    // Use AVX2 path
}
else if (Vector128.IsHardwareAccelerated)
{
    // Use SSE path
}
else
{
    // Scalar fallback
}
```

### Code Style
- **Comments**: Explain WHY, especially for performance tricks
- **Complexity**: Document O(n) for non-trivial algorithms
- **Safety**: Comment unsafe code with justification
- **Naming**: Follow existing conventions (PascalCase for public, _camelCase for private fields)

---

## Query Engine Architecture

### Core Concepts

**Column-Oriented Processing**
- Never materialize rows until final enumeration
- Operate on entire columns with SIMD
- Push predicates down to column level

**Chunk-Based Parallelism**
- Default chunk size: 16,384 rows (L2 cache optimized)
- Each thread processes independent chunks
- No synchronization required (lock-free)

**Optimization Layers**
1. **Zone Maps** - Skip entire chunks based on min/max
2. **Predicate Evaluation** - SIMD vectorized comparisons
3. **Fused Operations** - Single-pass filter+aggregate
4. **Parallel Execution** - Multi-core utilization

### Key Classes

| Class | Purpose | Optimization Points |
|-------|---------|---------------------|
| `SelectionBitmap` | Compact row selection (1 bit/row) | SIMD AND/OR, hardware popcount |
| `ColumnPredicate` | Pushdown predicates | SIMD comparisons, zone map testing |
| `ParallelQueryExecutor` | Chunk-based parallel eval | Work distribution, zone map skip |
| `FusedAggregator` | Single-pass filter+aggregate | Eliminate bitmap materialization |
| `ZoneMap` | Min/max per chunk | Skip-scanning for selective queries |

---

## Optimization Catalog

### Implemented ?
- SIMD predicate evaluation (Int32, Double)
- Parallel chunk-based execution
- Fused filter+aggregate operations
- Block-based bitmap iteration with TrailingZeroCount
- Hardware popcount for bit counting
- ArrayPool for temporary allocations
- Zone maps (min-max indices) for skip-scanning

### High Priority ??
1. **Null Bitmap Batch Processing** (Priority 2)
   - AND null bitmap with selection bitmap in bulk
   - Eliminate per-element IsNull checks
   - Expected: 5-10% improvement

2. **Predicate Reordering** (Priority 3)
   - Evaluate low-selectivity predicates first
   - Use zone map statistics for ordering
   - Expected: 10-20% for multi-predicate

3. **Expression Plan Caching** (Priority 4)
   - Cache analyzed query plans by expression
   - Eliminate repeated reflection
   - Expected: Faster query startup

### Medium Priority ??
4. **Bloom Filters for Strings**
   - Complement zone maps for string equality
   - Probabilistic skip-scanning
   
5. **Lazy Bitmap Materialization**
   - Stream evaluation for Any/First
   - Sparse index list for highly selective

6. **Vectorized Multi-Predicate**
   - Fuse multiple predicates in single SIMD pass
   - Reduce memory traffic

### Experimental ??
7. **Morsel-Driven Execution**
   - Pipeline parallelism with morsels (~1K-10K rows)
   - Overlap filter/aggregate stages
   
8. **JIT-Compiled Query Kernels**
   - Generate IL for repeated queries
   - Eliminate virtual calls

---

## Testing Guidelines

### Unit Tests
- Test correctness first, performance second
- Cover edge cases: empty, single element, large datasets
- Test with nullable and non-nullable columns

### Benchmarks (BenchmarkDotNet)
- Compare against `List<T>` LINQ as baseline
- Test multiple dataset sizes (10K, 100K, 1M)
- Use `[MemoryDiagnoser]` to track allocations
- Located in: `benchmarks/FrozenArrow.Benchmarks/`

### Profiling (Development Tool)
- Fast iteration for development-time verification
- Use profiling tool (see "Performance Verification Process" above)
- Always establish baseline before changes
- Located in: `profiling/FrozenArrow.Profiling/`

**Workflow:**
1. **Profiling Tool** - Development iteration (fast, focused)
2. **BenchmarkDotNet** - Final validation (statistical rigor)
3. **Unit Tests** - Correctness guarantee

---

## Communication Style

### When Proposing Optimizations

**Do:**
- ? Show understanding of current implementation
- ? Explain optimization technique clearly
- ? Rank options by impact/effort
- ? Provide expected performance improvements
- ? Consider edge cases and degradation

**Don't:**
- ? Propose without understanding existing code
- ? Implement without baseline capture
- ? Add complexity without measurable benefit
- ? Break APIs without strong justification
- ? Optimize without profiling/benchmarking

### Documentation Format

**For each optimization:**

```markdown
# [Optimization Name] Implementation

## What
Brief description of the optimization technique

## Why
What problem does it solve? When does it help?

## How
Technical explanation of the implementation

## Performance
Before/After numbers from profiling tool

## Trade-offs
What are the costs? When should it NOT be used?

## Future Work
What could be improved further?
```

---

## Anti-Patterns to Avoid

? **Premature Optimization**
- Don't optimize without profiling data
- Focus on proven bottlenecks

? **Over-Engineering**
- Simple solutions often perform best
- Avoid complexity that doesn't pay for itself

? **Ignoring Baselines**
- Always capture baseline before changes
- Verify improvements objectively

? **Breaking Compatibility**
- Maintain backward compatibility
- New features are opt-in

? **Allocating in Hot Paths**
- Use `ArrayPool`, `stackalloc`, or `Span<T>`
- Avoid LINQ in performance-critical code

? **Forgetting Documentation**
- Code without docs is incomplete
- Future maintainers will thank you

---

## References & Inspiration

Study these for optimization ideas:

- **Apache Arrow** - Columnar format, IPC
- **DuckDB** - Vectorized query engine, zone maps
- **ClickHouse** - Column-oriented DBMS, skip indices
- **Apache Parquet** - Row group statistics, encoding
- **Microsoft SQL Server** - Columnstore indexes
- **Polars** - DataFrame library (Rust/Python)

### Key Papers
- "MonetDB/X100: Hyper-Pipelining Query Execution" (vectorized execution)
- "Column-Stores vs. Row-Stores: How Different Are They Really?" (columnar benefits)
- "Efficiently Compiling Efficient Query Plans for Modern Hardware" (query compilation)

---

## Quick Reference

### Common Commands

```bash
# Build project
dotnet build

# Run unit tests
dotnet test

# Run profiling tool
cd profiling/FrozenArrow.Profiling
dotnet run -c Release -- -s all -r 1000000

# Run benchmarks
cd benchmarks/FrozenArrow.Benchmarks
dotnet run -c Release --filter *Filter*

# Check for compilation errors in specific file
dotnet build /p:CheckForOverflowUnderflow=true
```

### File Locations

```
src/FrozenArrow/               # Main library
  ??? Query/                   # Query engine
  ?   ??? ArrowQuery.cs        # LINQ provider
  ?   ??? ColumnPredicate.cs   # Predicate evaluation
  ?   ??? SelectionBitmap.cs   # Compact selection storage
  ?   ??? ZoneMap.cs           # Min-max indices
  ?   ??? ParallelQueryExecutor.cs
  ??? ...

benchmarks/FrozenArrow.Benchmarks/  # BenchmarkDotNet
  ??? FilterBenchmarks.cs
  ??? AggregationBenchmarks.cs
  ??? Internals/                     # Component benchmarks

profiling/FrozenArrow.Profiling/    # Profiling tool
  ??? Program.cs
  ??? README.md                      # Usage guide
  ??? baselines/                     # Saved baselines

docs/
  ??? optimizations/           # Optimization documentation
```

---

## Summary

This guide ensures:
- ? **Consistent process** for all optimization work
- ? **Objective verification** via profiling tool
- ? **Complete documentation** for maintainability
- ? **High code quality** with performance focus
- ? **Zero regressions** through mandatory baseline comparison

**Remember**: Every optimization must be measured. Intuition fails more often than it succeeds. Let the profiling tool guide the way! ??
