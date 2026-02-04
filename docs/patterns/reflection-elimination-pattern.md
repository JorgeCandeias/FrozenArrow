# Quick Reference: Reflection Elimination Pattern

## Before vs. After

### ? Before (Reflection-Based)

```csharp
// Slow: Uses reflection to access private fields/methods
var recordBatchField = typeof(FrozenArrow<>)
    .MakeGenericType(_elementType)
    .GetField("_recordBatch", BindingFlags.NonPublic | BindingFlags.Instance);

_recordBatch = (RecordBatch)recordBatchField!.GetValue(source)!;

var createItemMethod = sourceType.GetMethod("CreateItem", 
    BindingFlags.NonPublic | BindingFlags.Instance);
    
_createItem = (batch, index) => createItemMethod!.Invoke(source, [batch, index])!;
```

**Problems**:
- 6 reflection calls per query
- `Invoke()` boxes arguments (allocations)
- Prevents JIT inlining

---

### ? After (Direct Access)

```csharp
// Fast: Generic helper with direct property/method access
private static (RecordBatch, int, ...) ExtractSourceData<T>(object source)
{
    var typedSource = (FrozenArrow<T>)source;
    
    // Direct property access (no reflection)
    var recordBatch = typedSource.RecordBatch;
    var count = typedSource.Count;
    
    // Direct delegate (no Invoke boxing)
    Func<RecordBatch, int, object> createItem = 
        (batch, index) => typedSource.CreateItemInternal(batch, index)!;
    
    return (recordBatch, count, createItem);
}
```

**Benefits**:
- Only 1 reflection call (unavoidable generic bridging)
- No boxing/unboxing
- JIT can inline delegates
- 43% faster, 44% less allocations

---

## Pattern: Add Internal Accessors

### Step 1: Expose Internal API

```csharp
// In FrozenArrow<T>:
protected abstract T CreateItem(RecordBatch recordBatch, int index);

// Add internal accessor:
internal T CreateItemInternal(RecordBatch recordBatch, int index) 
    => CreateItem(recordBatch, index);
```

### Step 2: Use Generic Helper

```csharp
// In consuming code:
internal MyClass(object source)
{
    var elementType = ExtractElementType(source);
    
    // Call generic helper via reflection (once)
    var extractMethod = typeof(MyClass)
        .GetMethod(nameof(ExtractData), BindingFlags.NonPublic | BindingFlags.Static)!
        .MakeGenericMethod(elementType);
    
    var data = (MyData)extractMethod.Invoke(null, [source])!;
    
    // Use extracted data (no more reflection)
    _field1 = data.Field1;
    _field2 = data.Field2;
}

private static MyData ExtractData<T>(object source)
{
    var typed = (FrozenArrow<T>)source;
    return new MyData 
    {
        Field1 = typed.InternalAccessor1,
        Field2 = typed.InternalAccessor2,
        Delegate = (x, y) => typed.InternalMethod(x, y) // Direct!
    };
}
```

---

## Performance Impact

| Scenario | Improvement | Reason |
|----------|-------------|--------|
| Query Startup | 5-10% | Fewer reflection calls |
| Enumeration | **43%** | Eliminated `Invoke()` boxing |
| Memory | **44% less** | No boxing allocations |

---

## When to Apply

? **Use this pattern when**:
- Hot path accesses private members repeatedly
- Delegate creation happens in critical path
- Reflection prevents JIT optimizations
- Large-scale enumeration (amplifies per-call overhead)

? **Don't use when**:
- One-time initialization (negligible impact)
- Members are already public/internal
- Generic bridging impossible (complex type hierarchies)

---

## Gotchas

1. **Generic Method Overhead**: Still uses reflection for `MakeGenericMethod`, but only once
2. **Type Safety**: Cast can fail at runtime if source isn't expected type
3. **Maintenance**: Need to keep internal accessors in sync with implementation

---

## Testing Checklist

- [ ] Unit tests pass (verify correctness)
- [ ] Profiling shows improvement (verify performance)
- [ ] Build succeeds (verify no breaking changes)
- [ ] Baseline saved (document before/after)

---

**When in doubt, profile it!** Measure before and after to verify the optimization works.
