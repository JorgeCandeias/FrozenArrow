# Quick Reference: Null Bitmap Batch Processing Pattern

## Before vs. After

### ? Before (Per-Element Null Checks)

```csharp
// Slow: Check nulls inside hot loop
private void EvaluateSimd(Array array, ref SelectionBitmap selection)
{
    var values = array.Values;
    var nullBitmap = array.NullBitmapBuffer.Span;
    var hasNulls = array.NullCount > 0;

    // SIMD loop with null checks
    for (int i = 0; i < vectorEnd; i += 8)
    {
        var data = Vector256.LoadUnsafe(...);
        var mask = Vector256.GreaterThan(data, compareValue);
        
        // ? Extract null bits for these 8 elements
        // ? AND mask with null mask
        // ? Branch per element if unaligned
        ApplyMaskWithNullCheck(mask, ref selection, i, hasNulls, nullBitmap);
    }

    // Scalar tail with null checks
    for (; i < length; i++)
    {
        if (!selection[i]) continue;
        if (hasNulls && IsNull(nullBitmap, i))  // ? Branch + memory load
        {
            selection.Clear(i);
            continue;
        }
        // ... evaluate
    }
}
```

**Problems**:
- ~125,000 null checks per 1M rows
- Scattered null bitmap reads
- Branch mispredictions
- Cache misses

---

### ? After (Bulk Null Filtering)

```csharp
// Fast: Filter nulls upfront
private void EvaluateSimd(Array array, ref SelectionBitmap selection)
{
    var values = array.Values;
    var nullBitmap = array.NullBitmapBuffer.Span;
    var hasNulls = array.NullCount > 0;

    // ? OPTIMIZATION: Bulk null filtering (O(n/64) operations)
    if (hasNulls && !nullBitmap.IsEmpty)
    {
        selection.AndWithArrowNullBitmap(nullBitmap);
    }

    // SIMD loop: NO null checks, straight-line code
    for (int i = 0; i < vectorEnd; i += 8)
    {
        var data = Vector256.LoadUnsafe(...);
        var mask = Vector256.GreaterThan(data, compareValue);
        
        // ? No null checking - already filtered!
        ApplyMask(mask, ref selection, i);
    }

    // Scalar tail: NO null checks
    for (; i < length; i++)
    {
        if (!selection[i]) continue;  // Already includes null filter
        // ... evaluate (no null check needed)
    }
}
```

**Benefits**:
- 73.7% faster (Filter scenario)
- Zero null checks in hot loop
- Single sequential null bitmap pass
- Perfect branch prediction

---

## Pattern: Bulk Arrow Null Bitmap Filtering

### Step 1: Add to SelectionBitmap

```csharp
/// <summary>
/// ANDs selection bitmap with Arrow null bitmap in bulk.
/// Arrow format: byte array, LSB-first, 1 = valid (non-null), 0 = null.
/// </summary>
public void AndWithArrowNullBitmap(ReadOnlySpan<byte> arrowNullBitmap)
{
    // Convert Arrow bytes (8 bits) ? SelectionBitmap ulongs (64 bits)
    int byteIndex = 0;
    int blockIndex = 0;

    while (blockIndex < _blockCount && byteIndex + 7 < arrowNullBitmap.Length)
    {
        // Combine 8 bytes into 1 ulong (LSB-first)
        ulong nullBlock = arrowNullBitmap[byteIndex]
            | ((ulong)arrowNullBitmap[byteIndex + 1] << 8)
            | ((ulong)arrowNullBitmap[byteIndex + 2] << 16)
            | ((ulong)arrowNullBitmap[byteIndex + 3] << 24)
            | ((ulong)arrowNullBitmap[byteIndex + 4] << 32)
            | ((ulong)arrowNullBitmap[byteIndex + 5] << 40)
            | ((ulong)arrowNullBitmap[byteIndex + 6] << 48)
            | ((ulong)arrowNullBitmap[byteIndex + 7] << 56);

        // AND: clear bits where null
        _buffer![blockIndex] &= nullBlock;
        
        byteIndex += 8;
        blockIndex++;
    }

    // Handle tail (< 8 bytes remaining)
    // ...
}
```

### Step 2: Use Before Predicate Evaluation

```csharp
public override void Evaluate(RecordBatch batch, ref SelectionBitmap selection)
{
    var column = batch.Column(ColumnIndex);
    var array = (Int32Array)column;
    
    var values = array.Values;
    var nullBitmap = array.NullBitmapBuffer.Span;
    var hasNulls = array.NullCount > 0;

    // ? Bulk null filtering BEFORE evaluation
    if (hasNulls && !nullBitmap.IsEmpty)
    {
        selection.AndWithArrowNullBitmap(nullBitmap);
    }

    // Now evaluate without null checks
    EvaluateSimd(values, ref selection);
}
```

### Step 3: Simplify Hot Loop

```csharp
// Old signature (with null parameters)
private void ApplyMask(Vector256<int> mask, ref SelectionBitmap selection, 
                       int startIndex, bool hasNulls, ReadOnlySpan<byte> nullBitmap)
{
    var byteMask = (byte)Avx.MoveMask(mask.AsSingle());
    
    if (hasNulls)  // ? Branch
    {
        byteMask = ApplyNullMaskVectorized(byteMask, nullBitmap, startIndex);
    }
    
    selection.AndMask8(startIndex, byteMask);
}

// New signature (no null parameters!)
private void ApplyMask(Vector256<int> mask, ref SelectionBitmap selection, int startIndex)
{
    var byteMask = (byte)Avx.MoveMask(mask.AsSingle());
    
    // ? No null handling - already filtered!
    selection.AndMask8(startIndex, byteMask);
}
```

---

## Arrow Null Bitmap Format

```
Arrow stores nulls as bit-packed bytes:
  - 1 bit per value
  - LSB-first ordering (bit 0 = index 0)
  - 1 = valid (non-null), 0 = null

Example:
Index:     0  1  2  3  4  5  6  7
Value:    10 20 ? 40 50 ? ? 80
Bitmap:   [1  1  0  1  1  0  0  1]
          ??????????? Byte 0 = 0b11011001 = 0xD9

8 bytes ? 64 bits ? 1 ulong block in SelectionBitmap
```

---

## Performance Impact by Scenario

| Scenario | Null Checks Eliminated | Improvement | Reason |
|----------|------------------------|-------------|---------|
| Filter (3 predicates) | ~375,000 | **73.7%** | 3 predicates × compounding |
| Single Predicate | ~125,000 | **20-30%** | Single bulk filter |
| Sparse Aggregation | ~125,000 | **32.3%** | Filter + aggregate |
| Fused Execution | ~125,000 | **22.2%** | Integrated filter |

---

## When to Apply

? **Always apply when**:
- Working with Arrow arrays (null bitmaps present)
- Predicate evaluation hot loops
- Multiple predicates on nullable columns
- Non-parallel execution path

? **Consider carefully for**:
- Parallel execution with small chunks (may need per-element checks)
- Non-nullable columns (fast-path exit still beneficial)
- Custom array types (adapt pattern as needed)

? **Don't apply when**:
- No null bitmap present (array.NullCount == 0 fast-path)
- Bitmap format incompatible (not Arrow standard)
- Sub-range evaluation without access to full bitmap

---

## Common Pitfalls

### ? Forgetting Fast-Path Check

```csharp
// Bad: Always process even if no nulls
selection.AndWithArrowNullBitmap(nullBitmap);

// Good: Check first
if (hasNulls && !nullBitmap.IsEmpty)
{
    selection.AndWithArrowNullBitmap(nullBitmap);
}
```

### ? Wrong Bit Ordering

```csharp
// Bad: MSB-first (wrong!)
ulong block = ((ulong)bytes[0] << 56) | ((ulong)bytes[1] << 48) | ...;

// Good: LSB-first (matches Arrow)
ulong block = bytes[0] | ((ulong)bytes[1] << 8) | ((ulong)bytes[2] << 16) | ...;
```

### ? Forgetting Range Evaluation Path

```csharp
// Range evaluation (parallel path) may need separate handling
public override void EvaluateRange(IArrowArray column, ref SelectionBitmap selection, 
                                    int startIndex, int endIndex)
{
    // Can't bulk filter sub-range without coordination
    // Keep per-element null checks here OR
    // Pre-filter before parallel distribution
}
```

---

## Verification Checklist

- [ ] Unit tests pass (correctness)
- [ ] Profiling shows improvement (73% on Filter expected)
- [ ] No allocations added (check with profiler)
- [ ] Baseline captured and saved
- [ ] Documentation updated
- [ ] Fast-path for non-nullable columns
- [ ] Range evaluation handled correctly

---

## Example: Complete Implementation

```csharp
public sealed class Int32ComparisonPredicate : ColumnPredicate
{
    public override void Evaluate(RecordBatch batch, ref SelectionBitmap selection)
    {
        var column = batch.Column(ColumnIndex);
        
        if (column is Int32Array int32Array)
        {
            EvaluateInt32ArraySimd(int32Array, ref selection);
            return;
        }

        base.Evaluate(batch, ref selection);  // Fallback
    }

    private void EvaluateInt32ArraySimd(Int32Array array, ref SelectionBitmap selection)
    {
        var values = array.Values;
        var length = array.Length;
        var nullBitmap = array.NullBitmapBuffer.Span;
        var hasNulls = array.NullCount > 0;

        // ? OPTIMIZATION: Bulk null filtering
        if (hasNulls && !nullBitmap.IsEmpty)
        {
            selection.AndWithArrowNullBitmap(nullBitmap);
        }

        // SIMD path: no null checks
        if (Vector256.IsHardwareAccelerated && length >= 8)
        {
            var compareValue = Vector256.Create(Value);
            int i = 0;
            int vectorEnd = length - (length % 8);

            for (; i < vectorEnd; i += 8)
            {
                var data = Vector256.LoadUnsafe(...);
                var mask = Operator switch
                {
                    ComparisonOperator.GreaterThan => Vector256.GreaterThan(data, compareValue),
                    // ...
                };

                // ? No null parameters!
                ApplyMaskToBitmap(mask, ref selection, i);
            }

            // Scalar tail: no null checks
            for (; i < length; i++)
            {
                if (!selection[i]) continue;
                if (!EvaluateScalar(values[i]))
                {
                    selection.Clear(i);
                }
            }
        }
        else
        {
            // Scalar fallback: no null checks
            for (int i = 0; i < length; i++)
            {
                if (!selection[i]) continue;
                if (!EvaluateScalar(values[i]))
                {
                    selection.Clear(i);
                }
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ApplyMaskToBitmap(Vector256<int> mask, ref SelectionBitmap selection, int startIndex)
    {
        if (Avx2.IsSupported)
        {
            var byteMask = (byte)Avx.MoveMask(mask.AsSingle());
            selection.AndMask8(startIndex, byteMask);
        }
        else
        {
            ApplyMaskToBitmapScalar(mask, ref selection, startIndex);
        }
    }
}
```

---

## Key Takeaways

1. **Bulk operations beat per-element**: 64× theoretical speedup (O(n/64) vs O(n))
2. **Branch elimination is critical**: SIMD loves straight-line code
3. **Single-pass is faster**: Sequential access beats scattered reads
4. **Measure the impact**: 73.7% vs. expected 5-10% (7-15× better!)
5. **Composition matters**: Multiple predicates compound the benefit

**When in doubt, bulk filter upfront!** ??
