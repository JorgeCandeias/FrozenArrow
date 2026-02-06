# Query Engine Architecture - Internal Plan Representation

## Overview

This document describes FrozenArrow's new internal query representation that decouples the query engine from user-facing APIs (LINQ, SQL, JSON, etc.).

## Motivation

**Problem**: The current query engine is tightly coupled to LINQ Expression trees. This creates several issues:

1. **Optimization limitations**: Every engine optimization risks breaking LINQ semantics
2. **No alternative query APIs**: Can't easily add SQL, JSON, or other query languages
3. **Caching complexity**: Caching Expression trees is inefficient
4. **Testing difficulty**: Hard to test optimizations independent of LINQ quirks

**Solution**: Introduce an internal logical plan representation that:

- Is API-agnostic (works with any query language)
- Is optimizable (transformable without breaking user-facing APIs)
- Is cacheable (compact, serializable representation)
- Is testable (verify optimizer correctness independent of LINQ)

## Architecture Layers

```
???????????????????????????????????????????????????????
?  Frontend APIs (User-Facing)                        ?
?  - LINQ (existing)                                  ?
?  - SQL (future)                                     ?
?  - JSON DSL (future)                                ?
?  - Arrow Flight SQL (future)                        ?
???????????????????????????????????????????????????????
                        ?
              Parse/Translate
                        ?
???????????????????????????????????????????????????????
?  Logical Plan (WHAT to compute)                     ?
?  - Scan, Filter, Project, Aggregate, GroupBy        ?
?  - Limit, Offset                                    ?
?  - Immutable, API-agnostic representation           ?
???????????????????????????????????????????????????????
                        ?
               Optimize
                        ?
???????????????????????????????????????????????????????
?  Optimized Logical Plan                             ?
?  - Predicates reordered by selectivity              ?
?  - Fused operations identified                      ?
?  - Pushdown opportunities marked                    ?
???????????????????????????????????????????????????????
                        ?
          Create Physical Plan
                        ?
???????????????????????????????????????????????????????
?  Physical Plan (HOW to execute)                     ?
?  - SimdFilter, ParallelScan, FusedFilterAggregate   ?
?  - Zone map strategies, SIMD vectorization          ?
?  - Chunk sizes, parallel strategies                 ?
???????????????????????????????????????????????????????
                        ?
               Execute
                        ?
???????????????????????????????????????????????????????
?  Results                                            ?
???????????????????????????????????????????????????????
```

## Logical Plan Nodes

### Core Operations

| Node Type | Description | Example |
|-----------|-------------|---------|
| `ScanPlan` | Table scan (data source) | `FROM table` |
| `FilterPlan` | Row filtering | `WHERE Age > 25` |
| `ProjectPlan` | Column selection | `SELECT Name, Age` |
| `AggregatePlan` | Simple aggregate | `SELECT SUM(Price)` |
| `GroupByPlan` | Grouped aggregate | `GROUP BY Category` |
| `LimitPlan` | Row limit | `LIMIT 100` / `.Take(100)` |
| `OffsetPlan` | Skip rows | `OFFSET 50` / `.Skip(50)` |

### Properties

All logical plan nodes are:

- **Immutable**: Thread-safe by design, can be cached safely
- **Composable**: Form a tree structure via `Input` property
- **Self-describing**: Include schema and estimated row counts
- **Transformable**: Support visitor pattern for optimization

## Query Optimizer

The `LogicalPlanOptimizer` applies transformation rules:

### Current Optimizations

1. **Predicate Reordering**
   - Evaluates most selective predicates first
   - Uses zone map statistics when available
   - Reduces work for subsequent predicates

2. **Fused Operations Detection**
   - Identifies Filter ? Aggregate patterns
   - Physical planner will use `FusedAggregator`
   - Single-pass evaluation, no bitmap materialization

### Future Optimizations (Easy to Add!)

3. **Filter Pushdown** - Move filters closer to scan
4. **Projection Pushdown** - Only read needed columns
5. **Predicate Elimination** - Remove always-true predicates
6. **Constant Folding** - Evaluate constants at plan time
7. **Join Reordering** - Optimal join order
8. **Learned Optimization** - Use query history for better estimates

## LINQ Adapter Layer

The `LinqToLogicalPlanTranslator` converts Expression trees to logical plans:

```csharp
// User writes LINQ
var results = frozenArrow
    .AsQueryable()
    .Where(x => x.Age > 25 && x.Country == "USA")
    .Take(100);

// Translator produces logical plan
ScanPlan("MyTable")
  ? FilterPlan([Age > 25, Country == "USA"], selectivity=0.25)
  ? LimitPlan(100)

// Optimizer reorders predicates
ScanPlan("MyTable")
  ? FilterPlan([Country == "USA", Age > 25], selectivity=0.25)  // Country first (more selective)
  ? LimitPlan(100)

// Physical planner chooses execution strategy
ParallelScan + SimdFilter (AVX2) + EarlyExit(100)
```

The LINQ provider becomes a **thin translation layer** - easy to refactor without impacting the engine.

## Benefits

### 1. Optimization Freedom

**Before**: Every optimization risks breaking LINQ semantics  
**After**: Optimizer works on logical plans, LINQ users unaffected

Example: Predicate reordering is now a pure logical transformation.

### 2. Multiple Query Languages

**Before**: LINQ only  
**After**: Any language can translate to logical plans

```csharp
// LINQ
var results = data.Where(x => x.Age > 25);

// SQL (future)
var results = data.Query("SELECT * FROM data WHERE Age > 25");

// JSON (future)
var results = data.Query(new { filter: { age: { $gt: 25 } } });
```

All produce the same logical plan ? same optimized execution.

### 3. Expression Plan Caching

**Before**: Cache Expression trees (heavy, not canonical)  
**After**: Cache logical plans (light, canonical)

```csharp
// Cache key: logical plan hash
var cacheKey = logicalPlan.GetHashCode();
if (_cache.TryGet(cacheKey, out var physicalPlan))
{
    return _executor.Execute(physicalPlan);
}
```

### 4. Better Testing

**Before**: Test optimizations through LINQ  
**After**: Test optimizer directly on logical plans

```csharp
[Fact]
public void PredicateReordering_PlacesMostSelectiveFirst()
{
    // Arrange
    var plan = new FilterPlan(scan, [lowSelectivity, highSelectivity]);
    var optimizer = new LogicalPlanOptimizer(zoneMap);
    
    // Act
    var optimized = optimizer.Optimize(plan);
    
    // Assert
    Assert.Equal(highSelectivity, optimized.Predicates[0]);
}
```

### 5. Debugging & Visualization

**Before**: Debug Expression trees (complex, verbose)  
**After**: Visualize logical plans (clean, SQL-like)

```csharp
var plan = translator.Translate(expression);
Console.WriteLine(plan.Explain());

// Output:
// Scan(MyTable) [100,000 rows]
//   ? Filter(Age > 25, Country == "USA") [~25,000 rows]
//   ? Limit(100)
```

## Migration Path (Non-Breaking)

### Phase 1: Foundation (Current PR)
- ? Define logical plan types
- ? Implement optimizer skeleton
- ? Create LINQ translator stub
- Existing code unchanged, no impact to users

### Phase 2: Wire Up Translation
- Update `ArrowQueryProvider` to use translator
- Translate Expression ? LogicalPlan ? Execute
- Keep old path as fallback
- **Zero breaking changes**

### Phase 3: Move Optimizations
- Migrate predicate reordering to optimizer
- Migrate fused operations to optimizer
- Remove optimization logic from LINQ layer
- **Zero breaking changes** (optimizations are transparent)

### Phase 4: Enable Caching
- Implement logical plan hashing
- Add `QueryPlanCache` for logical plans
- Remove Expression tree caching
- **Zero breaking changes** (caching is transparent)

### Phase 5: Add New Frontends
- SQL query support
- JSON DSL support
- Arrow Flight SQL integration
- **Zero breaking changes** (additive only)

## File Structure

```
src/FrozenArrow/Query/
??? LogicalPlan/
?   ??? LogicalPlan.cs              # Base class + visitor interface
?   ??? ScanPlan.cs                 # Table scan
?   ??? FilterPlan.cs               # WHERE clause
?   ??? ProjectPlan.cs              # SELECT clause
?   ??? AggregatePlan.cs            # Simple aggregates
?   ??? GroupByPlan.cs              # GROUP BY + aggregates
?   ??? LimitOffsetPlan.cs          # LIMIT/OFFSET (Take/Skip)
?   ??? LogicalPlanOptimizer.cs     # Optimization rules
?   ??? LinqToLogicalPlanTranslator.cs  # LINQ ? LogicalPlan
??? ArrowQuery.cs                   # LINQ provider (existing)
??? QueryPlan.cs                    # Old plan representation (will deprecate)
??? ... (existing files)
```

## Example: End-to-End Flow

### User Code
```csharp
var results = frozenArrow
    .AsQueryable()
    .Where(x => x.Age > 25 && x.Country == "USA")
    .GroupBy(x => x.Category)
    .Select(g => new { Category = g.Key, Total = g.Sum(x => x.Sales) })
    .Take(10);
```

### Translation to Logical Plan
```
ScanPlan("Orders", 1M rows)
  ? FilterPlan([Age > 25, Country == "USA"], est. 250K rows)
  ? GroupByPlan(groupBy=Category, aggs=[Sum(Sales)], est. 100 groups)
  ? LimitPlan(10)
```

### Optimizer Transforms
```
ScanPlan("Orders", 1M rows)
  ? FilterPlan([Country == "USA", Age > 25], est. 250K rows)  // Reordered!
  ? GroupByPlan(groupBy=Category, aggs=[Sum(Sales)], est. 100 groups)
  ? LimitPlan(10)
```

### Physical Execution
```
ParallelScan(Orders, chunkSize=16K)
  ? SimdFilter(Country == "USA") + SimdFilter(Age > 25)  // Vectorized
  ? GroupedColumnAggregator(Category ? Sum(Sales))       // Column-only
  ? EarlyExit(10 groups found)                           // Stop early
```

### Result
- Optimized predicate order (Country first - more selective)
- Parallel SIMD evaluation
- Column-only grouping (no object materialization)
- Early exit after 10 groups

All transparent to the user!

## Design Principles

### 1. Immutability First
All logical plan nodes are immutable. Optimizer creates new plans rather than mutating.

**Why**: Thread-safety, safe caching, easier reasoning about transformations.

### 2. API Agnostic
Logical plans have no knowledge of LINQ, SQL, or any frontend.

**Why**: Easy to add new query languages without touching the engine.

### 3. Separation of Concerns
- **Frontend**: Parse user input ? Logical plan
- **Optimizer**: Logical plan ? Optimized logical plan
- **Physical Planner**: Logical plan ? Physical plan
- **Executor**: Physical plan ? Results

**Why**: Each layer can evolve independently.

### 4. Correctness First
Optimizations must preserve query semantics exactly.

**Why**: Users trust that `.Where(x => x.Age > 25)` always filters correctly, regardless of optimizations.

### 5. Transparency
Optimizations should be invisible to users (zero breaking changes).

**Why**: Users get performance improvements without code changes.

## Future Work

### Near-Term
- Complete LINQ translator (GroupBy, Select with projections)
- Physical plan representation
- Physical planner (logical ? physical)
- Update existing executors to consume logical plans

### Medium-Term
- SQL query support (`data.Query("SELECT * FROM data WHERE ...")`)
- Logical plan caching
- More optimization rules (pushdown, constant folding)
- Plan visualization tool

### Long-Term
- Join support (multi-table queries)
- Subquery support
- Arrow Flight SQL integration
- Learned query optimization (ML-based)

## Summary

The internal plan representation:

? **Decouples** query engine from user-facing APIs  
? **Enables** multiple query languages (LINQ, SQL, JSON)  
? **Simplifies** optimizer implementation and testing  
? **Improves** plan caching and debugging  
? **Maintains** zero breaking changes (transparent to users)  

This is the foundation for FrozenArrow to become a world-class query engine that rivals DuckDB and Polars.
