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

### Immutability First: Thread-Safety by Design

FrozenArrow is a frozen/immutable collection.
After creation, data structures must not be altered in any way.

**Design Philosophy:**
- ✅ **Immutable = Correct** - Immutable objects are inherently thread-safe
- ✅ **No Mutable Shared State** - Eliminate entire classes of concurrency bugs
- ✅ **Correctness > Performance** - For shared caches or coordination structures, choose thread-safety over speed
- ✅ **Fail-Fast Construction** - Objects should be fully initialized in their constructors

**Practical Guidelines:**

1. **Properties Should Be Immutable**
   ```csharp
   // ✅ GOOD - Immutable property
   public int ColumnIndex { get; }
   
   // ❌ BAD - Mutable property invites race conditions
   public int ColumnIndex { get; set; }
   ```

2. **Initialize in Constructor**
   ```csharp
   // ✅ GOOD - Fully initialized at construction
   public Int32ComparisonPredicate(string columnName, int columnIndex, ComparisonOperator op, int value)
   {
       ColumnName = columnName;
       ColumnIndex = columnIndex;  // Set once, never changed
       Operator = op;
       Value = value;
   }
   
   // ❌ BAD - Post-construction mutation
   public Int32ComparisonPredicate(string columnName, ComparisonOperator op, int value)
   {
       ColumnName = columnName;
       // ColumnIndex set later ← RACE CONDITION RISK!
   }
   ```

3. **Collections Should Be Read-Only After Construction**
   ```csharp
   // ✅ GOOD - Expose as IReadOnlyList
   public IReadOnlyList<ColumnPredicate> Predicates { get; }
   
   // ❌ BAD - Mutable list can be modified
   public List<ColumnPredicate> Predicates { get; }
   ```

4. **Shared Caches Must Be Thread-Safe**
   ```csharp
   // ✅ GOOD - ConcurrentDictionary is thread-safe
   private readonly ConcurrentDictionary<string, CacheEntry> _cache = new();
   
   // ❌ BAD - Dictionary is not thread-safe
   private readonly Dictionary<string, CacheEntry> _cache = new();
   ```

5. **Avoid Lazy Initialization of Shared State**
   ```csharp
   // ✅ GOOD - Initialize in constructor
   public QueryPlanCache()
   {
       _cache = new ConcurrentDictionary<string, CacheEntry>();
   }
   
   // ❌ BAD - Lazy init can have race conditions
   private ConcurrentDictionary<string, CacheEntry>? _cache;
   public ConcurrentDictionary<string, CacheEntry> Cache => _cache ??= new();
   ```

**When Mutation Is Unavoidable:**
- Use `Interlocked` operations for counters/timestamps (e.g., LRU tracking)
- Comment why mutation is necessary and thread-safety guarantees
- Consider if the mutable state can be moved to a separate coordination class

**Verification:**
- Ask: "Could two threads access this simultaneously?"
- Ask: "Is this object fully constructed before being shared?"
- Ask: "Can this design eliminate the need for locks?"

---

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

The guidelines in this document focus on *verification* (ensuring changes work), not on *limiting what you try*.
As a frontier AI model, your ability to make novel connections and challenge assumptions is one of your greatest strengths.

**When proposing optimizations, explore unconventional ideas:**

1. **Reverse the problem:**
- Instead of "how to filter faster," ask "can we avoid filtering?"
- Instead of "optimize this loop," ask "can we eliminate the loop?"
- Instead of "speed up allocation," ask "can we avoid allocating?"

2. **Borrow from other domains:**
- Graphics programming (texture compression, Z-buffer tricks)
- Database systems (learned indices, adaptive radix trees)
- Compiler optimization (loop fusion, strength reduction)
- Machine learning (learned query optimization)
- Hardware design (prefetching, speculation)

3. **Challenge fundamental assumptions:**
- "Why do we build the bitmap at all?"
- "Does this need to be sequential?"
- "What if we delayed this computation until actually needed?"
- "Could we use a completely different data structure?"
- "What if we processed data in a different order?"

4. **Propose high-risk, high-reward ideas:**
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
- **Safety rails** - Don't break things unknowingly
- **Quality gates** - Verify improvements objectively  
- **Context** - What's been tried, what worked
- **Shared language** - Communicate ideas clearly

**Don't let them become:**
- **Creativity limiters** - "We must do it this way"
- **The only path** - Other approaches exist
- **Barriers to experimentation** - Try bold ideas
- **Excuse for incrementalism** - Think bigger

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
dotnet run -c Release -- -s all -r 1000000 --save baseline-{optimization-name}.json
```

**Example naming:**
- `baseline-zone-maps.json`
- `baseline-null-bitmap-batch.json`
- `baseline-predicate-reorder.json`

**Store baselines in**: `profiling/FrozenArrow.Profiling/baselines/`

### 2. Implement Changes

- Make the optimization
- Ensure `dotnet build` succeeds
- Run relevant unit tests
- Add inline comments explaining non-obvious performance tricks

### 3. Verify Improvement (After Changes)

```bash
cd profiling/FrozenArrow.Profiling
dotnet run -c Release -- -s all -r 1000000 -c benchmark-{optimization-name}.json
```

**This will show:**
- **Improvements** (green): Scenarios that got faster
- **Regressions** (red): Scenarios that got slower
- **Neutral** (gray): Scenarios with <5% change

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

### Acceptance Criteria

- **Target scenario shows improvement** (>5% speedup)  
- **No unrelated scenarios regress** (>5% slowdown) without justification  
- **Memory allocation doesn't increase significantly** (>20%) without reason  
- **Improvement is documented** with before/after numbers  
- **Baseline saved** for future comparisons
- **Profiling README.md updated** if new scenario added or significant results changed

### Red Flags ??

**Stop and investigate if:**
- Target scenario shows <5% improvement (optimization may not be working)
- Multiple unrelated scenarios regress >10%
- Memory allocation increases >50% without clear justification
- Any scenario becomes >2x slower

### Dealing with Unstable Results ??

The profiler automatically detects and handles measurement instability:

1. **Outlier Removal**: Samples affected by GC or OS interruptions are automatically removed using IQR method. Output shows "X outlier(s) removed" when this occurs.

2. **Stability Warnings**: Results marked with ?? have high variance (CV > 15%) and may be unreliable.

**If you see unstable results:**
```bash
# For allocation-heavy scenarios (GroupBy, Enumeration), use GC between iterations:
dotnet run -c Release -- -s groupby -r 1000000 -i 10 --gc-between-iterations

# Increase iterations and warmup for more stable median:
dotnet run -c Release -- -s all -r 1000000 -i 15 -w 5

# To see raw data without outlier removal:
dotnet run -c Release -- -s all -r 1000000 -i 20 --no-outlier-removal
```

**Don't trust results that:**
- Show ?? stability warning
- Have max/min ratio > 2x
- Required many outliers to be removed (>30% of samples)

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

### Step 10: Create Optimization Documentation (REQUIRED)

**MANDATORY for all optimization work:**

1. **Determine next optimization number**:
   - Check `docs/optimizations/00-optimization-index.md` for last used number
   - Use next sequential number (e.g., if last is 14, use 15)

2. **Create main documentation** (`docs/optimizations/{NN}-{name}.md`):
   - Use **kebab-case** for name (e.g., `15-adaptive-query-execution.md`)
   - Follow template at `docs/optimizations/TEMPLATE.md`
   - Include all required sections:
     - Summary (1-2 sentences)
     - What Problem Does This Solve?
     - How It Works
     - Performance Characteristics
     - Implementation Details
     - Trade-offs
     - Related Optimizations
   - Include actual performance numbers from profiling verification
   - Add code snippets showing before/after

3. **Create summary (optional)** (`docs/optimizations/{NN}-{name}-summary.md`):
   - Only for major optimizations
   - High-level executive summary
   - Key performance numbers
   - Impact on different scenarios

4. **Update index** (`docs/optimizations/00-optimization-index.md`):
   - Add entry to optimization catalog table
   - Update "Total Optimizations" count at bottom
   - Add to appropriate synergy/pattern sections if applicable

5. **Create pattern doc if reusable** (`docs/patterns/{name}-pattern.md`):
   - Only if technique can be applied to other optimizations
   - Document general approach, not specific implementation
   - No numbering (patterns are alphabetical)

**File Naming Convention:**
```
docs/optimizations/{NN}-{optimization-name}.md           # Main doc (REQUIRED)
docs/optimizations/{NN}-{optimization-name}-summary.md   # Summary (OPTIONAL)
docs/patterns/{pattern-name}-pattern.md                  # Pattern (IF REUSABLE)
```

**Examples:**
- `15-adaptive-query-execution.md` - Main technical doc
- `15-adaptive-query-execution-summary.md` - Executive summary
- `adaptive-query-pattern.md` - Reusable pattern (in patterns folder)

---

## Code Quality Standards

### .NET Version Features
- **Prefer .NET 10 features** for performance code (main library)
- Use modern C# (C# 14.0): `ref struct`, `Span<T>`, pattern matching

### Code Style
- **Comments**: Explain WHY, especially for performance tricks
- **Complexity**: Document O(n) for non-trivial algorithms
- **Safety**: Comment unsafe code with justification
- **Naming**: Follow existing conventions (PascalCase for public, _camelCase for private fields)

---

## Testing Guidelines

### Unit Tests
- Test correctness first, performance second
- Cover edge cases: empty, single element, large datasets
- Test with nullable and non-nullable columns
- Cover concurrency scenarios if applicable
- Cover race conditions if mutable state is involved

### Benchmarks (BenchmarkDotNet)
- Compare against `List<T>` LINQ
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
- Show understanding of current implementation
- Explain optimization technique clearly
- Rank options by impact/effort
- Provide expected performance improvements
- Consider edge cases and degradation

**Don't:**
- Propose without understanding existing code
- Implement without baseline capture
- Add complexity without measurable benefit
- Break APIs without strong justification
- Optimize without profiling/benchmarking

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

---

## References & Inspiration

Study these for optimization ideas:

- **Apache Arrow** - Columnar format, IPC
- **DuckDB** - Vectorized query engine, zone maps
- **ClickHouse** - Column-oriented DBMS, skip indices
- **Apache Parquet** - Row group statistics, encoding
- **Microsoft SQL Server** - Columnstore indexes
- **Polars** - DataFrame library (Rust/Python)

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

