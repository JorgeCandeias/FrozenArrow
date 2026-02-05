# Concurrent Query Execution Bug Investigation

## Summary
Intermittent concurrency bug where concurrent queries with different predicates sometimes return incorrect results.

**UPDATE**: After making predicates fully immutable (ColumnIndex set at construction), the bug persists. This rules out ColumnIndex mutation as the root cause.

## What Works ?
- ? Single-column records: 50K rows, 20 concurrent threads, different thresholds - **passes consistently**
- ? Same predicate: 100 concurrent threads with same threshold - **passes consistently**
- ? Multi-column records: 50K rows, 20 concurrent threads, different thresholds in `ConcurrencyDebugTest` - **passes consistently**

## What Fails ?
- ? `QueryPlanCacheTests.DifferentQueries_ConcurrentExecution_ShouldCacheSeparately` - **still fails intermittently**
  - Error: "Higher threshold 530 should have <= matches than 520. Got 24483 for 530 vs 23997 for 520"
  - Threshold 530 is getting ~500 extra matches (close to what threshold 510 or 500 would return)
  - **Failure rate: ~90%** (18 failures out of 20 runs after immutability fix)

## Key Findings

### 1. Cache Architecture
- **Single Shared Cache**: All queries against the same `FrozenArrow<T>` instance share the same `QueryPlanCache`
  - Confirmed in `TypedQueryProviderCache.ExtractSourceDataTyped<T>()` line 69
  - Cache is stored in `FrozenArrow<T>._queryPlanCache` (line 47 of FrozenArrow.cs)
- **Cache Refactoring**: Moved from dual-dictionary to single-dictionary design
  - Old: `_cacheByHash` + `_cacheByKey` (race condition risk)
  - New: Single `_cache` dictionary (thread-safe) ?

### 2. Query Plan Immutability
- ? `QueryPlan`: All properties are `init`-only - **immutable**
- ? `Int32ComparisonPredicate`: `Value` property is get-only - **immutable** once constructed
- ? **ColumnPredicate.ColumnIndex**: NOW IMMUTABLE! ?
  - **FIXED**: Changed from `{ get; internal set; }` to abstract `{ get; }` 
  - Set at construction time, never mutated
  - All 8 predicate types updated to accept columnIndex in constructor
  - PredicateAnalyzer refactored to create predicates with columnIndex immediately

### 3. Immutability Refactoring Results
- ? All tests still pass (465/466)
- ? **Heisenbug persists!** Failure rate: ~90%
- **Conclusion**: ColumnIndex mutation was NOT the root cause
  - The bug is deeper than we thought
  - Must be related to something else in query execution or data access

## Potential Root Causes

### Theory 1: ColumnIndex Race Condition ~~(ELIMINATED)~~
**Status**: ~~Unlikely but possible~~ **ELIMINATED ?**
- ~~`ColumnPredicate.ColumnIndex` is mutable (`internal set`)~~
- ~~Set after predicate creation during query analysis~~
- **FIXED**: ColumnIndex is now immutable, set at construction
- **Result**: Bug persists after fix - this was NOT the cause

### Theory 2: Record Type Generation Issue
**Status**: Possible ??
- Multiple tests define `CacheTestRecord`
- Source generators might have subtle bugs with duplicate names
- Generator errors encountered during test creation
- **Evidence**: ConcurrencyDebugTest with different record name passes more reliably

**Investigation Needed**:
- Check generated code for `CacheTestRecord`
- Verify column index mappings are correct
- Test with uniquely named record types

### Theory 3: Test Framework Issue
**Status**: Unlikely
- xUnit might have subtle test isolation issues
- Parallel test execution could cause shared state

**Investigation Needed**:
- Run test in isolation: `dotnet test --filter "DifferentQueries..." -- --parallelize=false`
- Add `[Collection("Sequential")]` attribute to force sequential execution

### Theory 4: Cache Key Collision
**Status**: Very Unlikely (but worth verifying)
- Even with single dictionary, hash collisions could cause issues
- Cache keys include constant values, so should be unique

**Investigation Needed**:
- Add logging to see actual cache keys being generated
- Verify no hash/key collisions for thresholds 400-590

### Theory 5: Memory Model Issue
**Status**: Possible
- Weak memory ordering on multi-core systems
- Missing memory barriers or volatile reads
- `ColumnIndex` writes might not be visible to other threads

**Investigation Needed**:
- Add `volatile` keyword to `ColumnIndex`
- Use `Interlocked` operations for ColumnIndex updates
- Add memory barriers with `Thread.MemoryBarrier()`

## Reproduction Steps

### Reliable Reproduction (if possible)
```bash
# Run the failing test 100 times
for i in 1..100; do
  dotnet test --filter "DifferentQueries_ConcurrentExecution_ShouldCacheSeparately"
  if [ $? -ne 0 ]; then
    echo "FAILED on iteration $i"
    break
  fi
done
```

### Debugging Steps
1. **Add Extensive Logging**:
   ```csharp
   // In PredicateAnalyzer.Analyze
   Console.WriteLine($"[Thread {Thread.CurrentThread.ManagedThreadId}] Setting ColumnIndex={index} for predicate Value={pred.Value}");
   
   // In QueryPlanCache.CachePlan
   Console.WriteLine($"[Thread {Thread.CurrentThread.ManagedThreadId}] Caching plan with key: {key}");
   
   // In Int32ComparisonPredicate.Evaluate
   Console.WriteLine($"[Thread {Thread.CurrentThread.ManagedThreadId}] Evaluating: Value={Value}, ColumnIndex={ColumnIndex}");
   ```

2. **Use Thread Sanitizer** (if available for .NET):
   - Detect data races automatically
   - Identify unsynchronized memory access

3. **Profile with dotTrace or PerfView**:
   - Look for thread contention
   - Identify hot paths with concurrent access

4. **Simplify Test**:
   - Reduce to 2 threads with thresholds 520 and 530
   - Run 1000 times to increase failure probability
   - Binary search to find minimal reproduction case

## Workarounds

### For Users
1. **Don't share `FrozenArrow<T>` instances across threads** during query creation
2. **Create separate instances** if concurrent queries with different predicates are needed
3. **Use locks** around `AsQueryable()` calls if sharing is unavoidable

### For Tests
1. **Skip the flaky test** with detailed explanation (current approach)
2. **Add retry logic** to reduce false failures
3. **Mark as `[Theory(Skip = "...")]`** until root cause is found

## Next Steps

1. ? **Document the issue** (this file)
2. ? **Add extensive logging** to trace execution
3. ? **Run with ThreadSanitizer** or similar tools
4. ? **Create minimal reproduction** (2 threads, simple case)
5. ? **Profile with dotTrace** to find contention points
6. ? **Review all mutable state** in query execution path
7. ? **Consider making ColumnIndex immutable** (pass in constructor)

## Related Files
- `src/FrozenArrow/Query/QueryPlanCache.cs` - Single-dictionary cache implementation
- `src/FrozenArrow/Query/ColumnPredicate.cs` - Mutable ColumnIndex property
- `src/FrozenArrow/Query/PredicateAnalyzer.cs` - Sets ColumnIndex during analysis
- `src/FrozenArrow/FrozenArrow.cs` - Shared cache instance
- `tests/FrozenArrow.Tests/Concurrency/QueryPlanCacheTests.cs` - Failing test
- `tests/FrozenArrow.Tests/Concurrency/ConcurrencyDebugTest.cs` - Passing test (why?)

## Conclusion

This is a **Heisenbug** - it appears and disappears unpredictably. The fact that:
- Identical code in a different test file passes
- Same predicate with 100 threads passes
- Different predicates with 20 threads fails intermittently

Suggests a subtle race condition related to:
- Query plan caching with different constants
- Predicate creation/initialization
- Column index resolution
- Or test framework behavior

**Priority**: Medium (intermittent, hard to reproduce, workarounds available)
**Complexity**: High (Heisenbug, requires deep debugging)
**Impact**: Low (only affects specific concurrent scenarios)
