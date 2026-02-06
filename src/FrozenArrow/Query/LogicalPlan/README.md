# Logical Plan - Query Engine Core

This directory contains FrozenArrow's internal query representation layer, which decouples the query engine from user-facing APIs.

## Overview

**Problem**: The original query engine was tightly coupled to LINQ Expression trees, making it hard to:
- Add new query languages (SQL, JSON, etc.)
- Optimize queries without breaking LINQ semantics
- Cache query plans efficiently
- Test optimizer logic independently

**Solution**: Introduce an internal logical plan representation that:
- Is API-agnostic (works with any query language)
- Is immutable (thread-safe, cacheable)
- Is optimizable (transformable without API changes)
- Is testable (verify optimizations independently)

## Architecture

```
User Query (LINQ/SQL/JSON)
          ?
    [Translator]
          ?
   Logical Plan ? YOU ARE HERE
          ?
    [Optimizer]
          ?
  Optimized Logical Plan
          ?
  [Physical Planner]
          ?
   Physical Plan
          ?
    [Executor]
          ?
      Results
```

## Files

| File | Purpose |
|------|---------|
| `LogicalPlan.cs` | Base class + visitor interface |
| `ScanPlan.cs` | Table scan (data source) |
| `FilterPlan.cs` | WHERE clause (predicates) |
| `ProjectPlan.cs` | SELECT clause (projections) |
| `AggregatePlan.cs` | Simple aggregates (SUM, COUNT, etc.) |
| `GroupByPlan.cs` | GROUP BY with aggregations |
| `LimitOffsetPlan.cs` | LIMIT/OFFSET (Take/Skip) |
| `LogicalPlanOptimizer.cs` | Query optimizer (transformations) |
| `LinqToLogicalPlanTranslator.cs` | LINQ ? LogicalPlan translator |
| `LogicalPlanExample.cs` | Usage examples |

## Key Concepts

### Immutability
All logical plan nodes are **immutable**. Optimizers create new plans rather than mutating.

```csharp
// Properties are get-only, set in constructor
public sealed class FilterPlan : LogicalPlanNode
{
    public FilterPlan(LogicalPlanNode input, IReadOnlyList<ColumnPredicate> predicates)
    {
        Input = input;
        Predicates = predicates;
    }
    
    public LogicalPlanNode Input { get; }  // Immutable!
    public IReadOnlyList<ColumnPredicate> Predicates { get; }  // Immutable!
}
```

### Composability
Plans form a tree via the `Input` property:

```csharp
var plan = new LimitPlan(
    input: new FilterPlan(
        input: new ScanPlan(...),
        predicates: [...]
    ),
    count: 100
);
```

### Visitor Pattern
All nodes support transformation via visitor pattern:

```csharp
public interface ILogicalPlanVisitor<out TResult>
{
    TResult Visit(ScanPlan plan);
    TResult Visit(FilterPlan plan);
    // ... etc
}
```

This enables:
- **Optimization**: Transform plans without changing user APIs
- **Visualization**: Generate human-readable explanations
- **Analysis**: Extract statistics, cost estimates, etc.

## Usage Examples

### Creating a Plan Directly

```csharp
// Equivalent to: SELECT * FROM Orders WHERE Age > 25 LIMIT 100
var scan = new ScanPlan("Orders", sourceRef, schema, rowCount);
var filter = new FilterPlan(scan, predicates, selectivity: 0.5);
var limit = new LimitPlan(filter, 100);
```

### Optimizing a Plan

```csharp
var optimizer = new LogicalPlanOptimizer(zoneMap);
var optimized = optimizer.Optimize(plan);
// Predicates reordered by selectivity
// Fused operations identified
```

### Visualizing a Plan

```csharp
var explainer = new LogicalPlanExplainer();
Console.WriteLine(explainer.Explain(plan));

// Output:
// Limit(100) ? 100 rows
//   Filter(2 predicates) ? 50,000 rows
//     Scan(Orders) ? 1,000,000 rows
```

### Translating from LINQ

```csharp
var translator = new LinqToLogicalPlanTranslator(...);
var plan = translator.Translate(linqExpression);
// Expression tree ? Logical plan
```

## Optimization Rules

The `LogicalPlanOptimizer` applies transformation rules:

### Current Rules

1. **Predicate Reordering** (Implemented)
   - Evaluates most selective predicates first
   - Uses zone map statistics when available
   - Reduces work for subsequent predicates

2. **Fused Operations Detection** (Implemented)
   - Identifies Filter ? Aggregate patterns
   - Physical planner will use `FusedAggregator`
   - Single-pass evaluation

### Future Rules (Easy to Add!)

3. **Filter Pushdown** - Move filters closer to scan
4. **Projection Pushdown** - Only read needed columns
5. **Predicate Elimination** - Remove always-true predicates
6. **Constant Folding** - Evaluate constants at plan time
7. **Join Reordering** - Optimal join order (when joins added)

## Benefits

### For Optimization
- Optimizations are **pure transformations** on logical plans
- No risk of breaking LINQ semantics
- Easy to test (input plan ? expected output plan)
- Transparent to users

### For Multiple Query Languages

```csharp
// LINQ
var plan1 = linqTranslator.Translate(linqQuery);

// SQL (future)
var plan2 = sqlTranslator.Translate("SELECT * FROM data WHERE Age > 25");

// JSON (future)
var plan3 = jsonTranslator.Translate(jsonQuery);

// All produce the same logical plan!
// All use the same optimizer!
// All execute the same way!
```

### For Caching

```csharp
// Cache logical plans (canonical, lightweight)
var cacheKey = plan.GetHashCode();
if (cache.TryGet(cacheKey, out var physicalPlan))
{
    return executor.Execute(physicalPlan);
}
```

### For Testing

```csharp
[Fact]
public void Optimizer_ReordersPredicates_BySelectivity()
{
    // Arrange
    var plan = new FilterPlan(scan, [low, high]);
    var optimizer = new LogicalPlanOptimizer(zoneMap);
    
    // Act
    var optimized = optimizer.Optimize(plan);
    
    // Assert
    Assert.Equal(high, optimized.Predicates[0]);  // Most selective first
}
```

## Implementation Status

### ? Phase 1: Foundation (Complete)
- Logical plan types defined
- Optimizer skeleton implemented
- LINQ translator stub created
- Documentation written

### ? Phase 2: Integration (Future)
- Complete LINQ translator
- Wire up ArrowQueryProvider
- Fallback to old path for compatibility

### ? Phase 3: Physical Plans (Future)
- Define physical plan types
- Implement physical planner
- Update executors

### ? Phase 4: Caching (Future)
- Logical plan hashing
- QueryPlanCache implementation
- Benchmark improvements

### ? Phase 5: New Frontends (Future)
- SQL support
- JSON DSL support
- Arrow Flight SQL integration

## Design Principles

1. **Immutability First**: All plans are thread-safe by design
2. **API Agnostic**: No knowledge of any query language
3. **Separation of Concerns**: Logical (WHAT) vs Physical (HOW)
4. **Correctness**: Optimizations preserve semantics exactly
5. **Transparency**: Users unaware of optimizations

## Related Documentation

- `docs/architecture/query-engine-logical-plans.md` - Full architecture guide
- `docs/architecture/query-engine-refactoring-phase1-summary.md` - Phase 1 summary
- `LogicalPlanExample.cs` - Code examples

## Contributing

When adding new plan types:

1. **Inherit from `LogicalPlanNode`**
2. **Make all properties immutable** (get-only, set in constructor)
3. **Implement `Accept<TResult>()`** for visitor pattern
4. **Add to `ILogicalPlanVisitor<TResult>`** interface
5. **Update optimizer** if optimization applies
6. **Add tests** for new plan type and optimizations
7. **Document** the new plan type clearly

Example:

```csharp
public sealed class JoinPlan : LogicalPlanNode  // 1. Inherit
{
    public JoinPlan(LogicalPlanNode left, LogicalPlanNode right, JoinType type)
    {
        Left = left;   // 2. Set in constructor
        Right = right;
        Type = type;
    }
    
    public LogicalPlanNode Left { get; }    // 2. Immutable
    public LogicalPlanNode Right { get; }   // 2. Immutable
    public JoinType Type { get; }           // 2. Immutable
    
    public override TResult Accept<TResult>(ILogicalPlanVisitor<TResult> visitor)
    {
        return visitor.Visit(this);  // 3. Visitor pattern
    }
    
    // ... other required properties
}
```

Then update `ILogicalPlanVisitor<TResult>`:

```csharp
public interface ILogicalPlanVisitor<out TResult>
{
    // ... existing methods
    TResult Visit(JoinPlan plan);  // 4. Add to interface
}
```

## Summary

This directory contains the **heart of FrozenArrow's query engine**: the internal representation that decouples query languages from execution strategies.

**Key Achievement**: Clean separation enabling multiple query languages, easier optimization, and transparent performance improvements.

**Current Status**: Foundation complete, integration pending.

**Future**: This architecture enables FrozenArrow to become a world-class multi-language query engine.
