# Null Bitmap Batch Processing Optimization

## What
Bulk-AND the Arrow null bitmap with the selection bitmap before aggregation, eliminating per-element `IsNull()` checks in inner loops.

## Why
In aggregation operations, each row must check if the value is null before including it in the aggregate (Sum, Average, Min, Max). The traditional approach:

```csharp
// Before: Per-element null check in inner loop
while (block != 0)
{
    int bitIndex = TrailingZeroCount(block);
    int rowIndex = blockStartBit + bitIndex;
    
    if (!IsNull(nullBitmap, rowIndex))  // Branch in hot path!
    {
        sum += values[rowIndex];
    }
    block &= block - 1;
}
```

This introduces:
- **Branch misprediction**: The null check is unpredictable when nulls are sparse
- **Memory latency**: Reading the null bitmap byte-by-byte
- **Additional instructions**: Bit extraction per element

## How
Pre-apply the null bitmap to the selection bitmap in a single O(n/64) pass:

```csharp
// New: Bulk AND with SIMD (processes 256 bits at a time with AVX2)
selection.AndWithNullBitmap(array.NullBitmapBuffer.Span, hasNulls);

// Then: Branchless inner loop
while (block != 0)
{
    int bitIndex = TrailingZeroCount(block);
    int rowIndex = blockStartBit + bitIndex;
    sum += values[rowIndex];  // No null check needed!
    block &= block - 1;
}
```

### Implementation Details

1. **SelectionBitmap.AndWithNullBitmap()**
   - Converts Arrow's byte-oriented bitmap to ulong blocks
   - Uses AVX2 SIMD to process 256 bits (32 bytes) at once
   - Handles partial blocks at boundaries correctly

2. **BlockBasedAggregator methods**
   - Added `nullsPreApplied` parameter to all aggregation methods
   - When true, skips per-element null checks entirely
   - Maintains backward compatibility with default `false` value

3. **ParallelAggregator**
   - Applies null bitmap once before the parallel loop
   - Each thread gets a clean selection buffer with nulls pre-masked

## Performance

### Test Environment
- Windows 11, .NET 10.0, 24-core CPU, AVX2 enabled
- 1,000,000 rows, 10 iterations

### Results (non-nullable columns - worst case)

| Scenario | Before (µs) | After (µs) | Improvement |
|----------|-------------|------------|-------------|
| **SparseAggregation** | 34,518 | 29,521 | **14.5%** |
| **FusedExecution** | 11,176 | 10,540 | **5.7%** |
| **Aggregate** | 3,517 | 3,391 | **3.6%** |

### Expected Results (nullable columns)
With data that has 10-20% nulls:
- **5-15% additional improvement** due to eliminated branch misprediction
- **Reduced memory bandwidth** from avoiding per-element null bitmap reads

## Trade-offs

### Pros
- ? Eliminates branches in inner loop (improves instruction pipelining)
- ? Uses SIMD for bulk null masking (256+ bits per instruction)
- ? Works with existing SelectionBitmap infrastructure
- ? No additional memory allocation (reuses existing bitmap)
- ? Backward compatible API

### Cons
- ?? Requires single-threaded pass before parallel aggregation
- ?? Small overhead for non-nullable columns (but still faster overall)
- ?? Modifies selection bitmap (can't reuse for multiple aggregations on same column)

## When to Use
- **Always**: The optimization is automatically applied in ParallelAggregator
- **Best gains**: Columns with >5% null values
- **Still beneficial**: Non-nullable columns (cleaner code paths)

## Future Work
1. **Apply to predicate evaluation**: Pre-mask nulls in SIMD predicate loops
2. **Column-specific null masking**: Only mask nulls for the column being aggregated
3. **Lazy null masking**: Only mask when entering aggregation phase
4. **AVX-512 support**: Process 512 bits at a time when available

## Code Locations
- `SelectionBitmap.AndWithNullBitmap()` - Core bulk AND implementation
- `BlockBasedAggregator.*BlockBased()` - Aggregation methods with `nullsPreApplied`
- `ParallelAggregator.*Parallel()` - Integration points
