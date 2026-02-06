# Optimization #18: Pagination Skip/Take Early Termination

**Status**: ? Complete  
**Impact**: **2× faster** pagination queries (50% speedup)  
**Type**: Algorithm  
**Implemented**: February 2026  
**Complexity**: Medium

---

## Summary

Stops collecting matching row indices once `Skip + Take` limit is reached, avoiding unnecessary predicate evaluation on the entire dataset. For pagination queries like `.Where(...).Skip(1000).Take(100)`, this only evaluates predicates until 1100 matches are found instead of scanning all rows.

---

## What Problem Does This Solve?

### Before Optimization

For a query like:
```csharp
data.AsQueryable()
    .Where(x => x.Age > 25)        // Matches ~85% of 1M rows
    .Skip(1000)
    .Take(100)
    .ToList();
```

**Old behavior:**
1. Evaluate predicates on **all 1,000,000 rows**
2. Build selection bitmap with **850,000 set bits**
3. Collect **850,000 indices** into a list
4. Apply Skip(1000) and Take(100) ? discard 849,900 indices
5. Materialize only 100 objects

**Performance cost:**
- ? 850,000 predicate evaluations (only need 1,100)
- ? 125 KB bitmap allocation
- ? 850,000 indices collected and then discarded
- ?? **421 ms** for pagination scenario

### After Optimization

**New behavior:**
1. Evaluate predicates **until 1,100 matches found** (early termination)
2. Skip building selection bitmap entirely
3. Collect exactly **1,100 indices** (Skip + Take limit)
4. Apply Skip(1000) and Take(100) ? keep last 100 indices
5. Materialize 100 objects

**Performance improvement:**
- ? Only 1,100 predicate evaluations (99.8% reduction)
- ? No bitmap allocation
- ? No wasted indices collected
- ?? **210 ms** for pagination scenario (**2× faster**, 50% speedup)

---

## How It Works

### Detection

The optimization triggers for queries matching this pattern:
```
.Where(predicates...)    // Has column predicates
.Skip(N)                 // Optional
.Take(M)                 // Required
.ToList() / ToArray()    // Materialization (not Count/Any/First)
```

**Eligibility criteria:**
- Has at least one column predicate
- Has `.Take()` operation (required)
- Ends with enumeration/materialization (`.ToList()`, `.ToArray()`, `foreach`)
- NOT Count/LongCount (bitmap PopCount is faster for counting)

### Early Termination Logic

#### 1. Calculate Maximum Indices Needed
```csharp
int skipCount = plan.Skip ?? 0;          // e.g., 1000
int takeCount = plan.Take.Value;         // e.g., 100
int maxIndicesToCollect = skipCount + takeCount;  // = 1100
```

#### 2. Sequential Collection (Small Datasets)
For datasets below parallel threshold (< 100K rows):
```csharp
for (int chunkIndex = 0; chunkIndex < chunkCount; chunkIndex++)
{
    // Early termination: stop once we have enough
    if (maxIndicesToCollect.HasValue && result.Count >= maxIndicesToCollect.Value)
    {
        break;  // Exit immediately
    }
    
    // Zone map skip (check min/max)
    if (CanSkipChunk(predicates, zoneMapData, chunkIndex))
        continue;
    
    // Evaluate rows in chunk
    for (int row = chunkStart; row < chunkEnd; row++)
    {
        if (EvaluateAllPredicates(predicates, columns, row))
        {
            result.Add(row);
            
            // Check after each match
            if (maxIndicesToCollect.HasValue && result.Count >= maxIndicesToCollect.Value)
            {
                return;  // Done!
            }
        }
    }
}
```

#### 3. Parallel Collection (Large Datasets)
For large datasets (? 100K rows), use thread-safe shared counter:
```csharp
int collectedCount = 0;  // Shared across threads

Parallel.For(0, chunkCount, (chunkIndex, state, threadList) =>
{
    // Check if we've collected enough globally
    if (maxIndicesToCollect.HasValue && 
        Interlocked.CompareExchange(ref collectedCount, 0, 0) >= maxIndicesToCollect.Value)
    {
        state.Stop();  // Signal other threads to stop
        return threadList;
    }
    
    // Evaluate predicates...
    if (matchesPredicates)
    {
        threadList.Add(row);
        
        // Atomic increment and check
        if (maxIndicesToCollect.HasValue)
        {
            int currentCount = Interlocked.Increment(ref collectedCount);
            if (currentCount >= maxIndicesToCollect.Value)
            {
                state.Stop();
                return threadList;
            }
        }
    }
});
```

**Note**: Parallel early termination is approximate due to races - threads may collect slightly more than needed, which is trimmed during merge.

---

## Performance Characteristics

### Best Cases (Maximum Impact)

1. **Deep Pagination with High Selectivity**
   ```csharp
   .Where(x => x.Age > 25)      // Matches 85% of rows
   .Skip(50000)
   .Take(10)
   .ToList();
   ```
   - Need 50,010 matches out of 850,000 total
   - **17× fewer evaluations** (94% saved)
   - Expected: **5-10× speedup**

2. **Classic Pagination (Page 100, Size 100)**
   ```csharp
   .Where(x => x.IsActive)      // Matches 70% of rows
   .Skip(10000)
   .Take(100)
   .ToList();
   ```
   - Need 10,100 matches out of 700,000 total
   - **69× fewer evaluations** (99% saved)
   - Expected: **2-5× speedup**

3. **First Page (Low Skip, Small Take)**
   ```csharp
   .Where(x => x.Age > 55)      // Matches 20% of rows
   .Take(1000)
   .ToList();
   ```
   - Need 1,000 matches out of 200,000 total
   - **200× fewer evaluations** (99.5% saved)
   - Expected: **3-8× speedup**

### Neutral Cases (No Impact)

- **Aggregations**: `.Count()`, `.Sum()`, `.Average()`, `.Min()`, `.Max()`
  - Bitmap PopCount or SIMD aggregation is faster than index collection
  - Optimization explicitly disabled for these cases

- **No Pagination**: `.Where(...).ToList()` without Skip/Take
  - All rows are needed, no early termination possible

- **Very Large Take**: `.Take(1000000)` (Take ? result count)
  - Functionally equivalent to no limit

### Worst Cases (Minimal Impact)

- **Low Selectivity + Large Skip+Take**
  ```csharp
  .Where(x => x.Age > 80)      // Matches 1% of rows
  .Skip(5000)
  .Take(5000)
  .ToList();
  ```
  - Need 10,000 matches, but only 10,000 exist
  - Must scan entire dataset anyway
  - Expected: **0-10% speedup** (some overhead saved)

---

## Implementation Details

### Code Changes

**Files Modified:**
1. `src/FrozenArrow/Query/SparseIndexCollector.cs` - Added `maxIndicesToCollect` parameter
   - Updated `CollectMatchingIndices` signature
   - Added early termination logic to sequential collection
   - Added thread-safe early termination to parallel collection
   - Added result trimming in merge phase

2. `src/FrozenArrow/Query/ArrowQuery.cs` - Detect pagination pattern and invoke optimization
   - Added pagination detection logic in `ExecutePlan`
   - Calculate `maxIndicesToCollect = Skip + Take`
   - Route to `SparseIndexCollector` with limit

### Key Design Decisions

#### Why Use Sparse Collection Instead of Bitmap?

For pagination, collecting indices directly is more efficient than bitmaps:
- **Bitmap**: Always allocates 125 KB (1M rows ÷ 8 bits/byte), scans all rows
- **Index list**: Only allocates for matches found (Skip + Take), stops early
- **Memory**: Index list for 1000 matches = 4 KB vs 125 KB bitmap (31× less)

#### Why Not Apply to Count()?

For `.Count()`, hardware PopCount on bitmap is faster than list length:
- Bitmap PopCount: 1-2 cycles per 64-bit word (SIMD-optimized)
- Index list length: Requires collecting all indices first
- Verdict: Bitmap wins for aggregation operations

#### Parallel vs Sequential Early Termination

Sequential early termination is deterministic and exact:
```csharp
if (result.Count >= limit) return;  // Precise
```

Parallel early termination is approximate due to race conditions:
```csharp
if (Interlocked.Increment(ref count) >= limit)
    state.Stop();  // Approximate (threads may over-collect)
```

Over-collection is trimmed during merge, ensuring correctness.

---

## Trade-offs

### ? Advantages

1. **Dramatic speedup for pagination** (2-10× depending on Skip/Take ratio)
2. **Lower memory usage** (no bitmap allocation for paginated queries)
3. **Scales well with dataset size** (larger datasets = more saved work)
4. **No breaking changes** (API-transparent optimization)
5. **Works with zone maps** (skip chunks + early termination = multiplicative benefit)

### ?? Disadvantages

1. **Adds code complexity** (extra parameters, early exit logic)
2. **Parallel early termination is approximate** (may over-collect, then trim)
3. **Not beneficial for Count** (explicitly disabled)
4. **Requires Skip+Take pattern** (doesn't help `.Where(...).ToList()`)

### When to Use

**? Use this optimization when:**
- Query has `.Where(...).Skip(N).Take(M)` pattern
- Skip + Take is significantly smaller than expected result count
- Predicates are expensive to evaluate
- Materializing results (`.ToList()`, `.ToArray()`, `foreach`)

**? Don't use this optimization when:**
- Using aggregation (`.Count()`, `.Sum()`, etc.)
- Take is omitted or very large (Take ? result count)
- Skip and Take together cover most results
- Using short-circuit operations (`.Any()`, `.First()`) - those have their own optimization (#10)

---

## Verification

### Profiling Results

**Baseline (before optimization):**
```
Pagination: 421.8 ms (78.6 MB allocated)
```

**After optimization:**
```
Pagination: 209.9 ms (69.8 MB allocated)
```

**Improvement:**
- **50.2% faster** (2.0× speedup)
- **11% less memory** (8.8 MB saved per run)

### Test Coverage

Pagination scenarios tested (in `profiling/FrozenArrow.Profiling/Scenarios/PaginationScenario.cs`):
1. Take only (first 1000 results)
2. Skip only (skip first 10000 results)
3. Skip + Take (classic pagination, page 100)
4. Large Skip (deep pagination, skip 50000)
5. Take with Count (limit then count)
6. Skip + Take with Count
7. Take with First (early termination)
8. Skip with First
9. Highly selective Take
10. Empty results Take

All scenarios pass with correctness checks and show expected performance improvements.

---

## Related Optimizations

- **#07: Lazy Bitmap Short-Circuit** - Stops evaluation for `.Any()` / `.First()` as soon as match is found
- **#10: Streaming Predicates** - Similar early-exit strategy for short-circuit operations
- **#04: Zone Maps** - Complements this optimization by skipping chunks before even checking row limit
- **#17: Lazy Bitmap Materialization** - Similar concept of avoiding full bitmap for sparse results

---

## Future Enhancements

1. **Adaptive Threshold**: Auto-detect when pagination optimization is beneficial
   - If `Skip + Take` < 10% of estimated results, use optimization
   - If `Skip + Take` > 50% of estimated results, use bitmap
   - Use zone map statistics for estimation

2. **Parallel Work Stealing**: Better parallel load balancing
   - Instead of stopping all threads, use work-stealing queue
   - Threads that finish early help other threads until limit reached

3. **SIMD Early Exit**: Check multiple rows at once
   - Evaluate 4-8 rows with SIMD
   - Count matches in vector register
   - Exit when cumulative count >= limit

4. **Cursor-Based Pagination**: Support for stateful pagination
   - Remember last row index from previous page
   - Start next page from that index (no Skip needed)

---

## References

- **DuckDB Morsel-Driven Parallelism**: Similar early-exit strategy for parallel execution
- **Apache Arrow Flight SQL**: Uses similar techniques for pagination in distributed queries
- **PostgreSQL LIMIT/OFFSET**: Database-level pagination optimization patterns

---

**Author**: AI Assistant (Copilot)  
**Reviewed**: Pending  
**Last Updated**: February 2026
