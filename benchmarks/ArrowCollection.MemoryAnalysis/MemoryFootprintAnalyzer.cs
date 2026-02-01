using System.Runtime.InteropServices;
using ArrowCollection;

namespace ArrowCollection.MemoryAnalysis;

/// <summary>
/// Analyzes the long-term memory footprint of ArrowCollection vs List.
/// This is separate from BenchmarkDotNet because steady-state memory measurement
/// requires a different approach than allocation/timing benchmarks.
/// </summary>
/// <remarks>
/// <para>
/// BenchmarkDotNet's MemoryDiagnoser measures allocations during execution,
/// not retained memory after GC. This harness provides both theoretical analysis
/// and empirical measurements for collections held in memory long-term.
/// </para>
/// <para>
/// Note: ArrowCollection uses native memory via Apache Arrow's NativeMemoryAllocator.
/// Native memory is not tracked by GC.GetTotalMemory(), so we provide estimates.
/// </para>
/// </remarks>
public static class MemoryFootprintAnalyzer
{
    /// <summary>
    /// Runs the memory footprint analysis and prints results.
    /// </summary>
    public static void Run()
    {
        Console.WriteLine();
        Console.WriteLine("+==============================================================================+");
        Console.WriteLine("|              LONG-TERM MEMORY FOOTPRINT ANALYSIS                             |");
        Console.WriteLine("|  Compares retained memory for List<T> vs ArrowCollection                     |");
        Console.WriteLine("+==============================================================================+");
        Console.WriteLine();

        // Theoretical analysis
        Console.WriteLine("THEORETICAL MEMORY ANALYSIS");
        Console.WriteLine("===========================");
        Console.WriteLine();
        RunTheoreticalAnalysis();

        Console.WriteLine();
        Console.WriteLine("EMPIRICAL MEMORY ANALYSIS (Managed Heap)");
        Console.WriteLine("=========================================");
        Console.WriteLine();
        Console.WriteLine("Note: ArrowCollection stores data in native memory (not GC-tracked).");
        Console.WriteLine("      Managed heap shows only the .NET wrapper overhead.");
        Console.WriteLine();

        // Warm up
        WarmUp();

        // Empirical measurements
        RunEmpiricalAnalysis();

        // Cardinality impact
        Console.WriteLine();
        Console.WriteLine("STRING CARDINALITY IMPACT");
        Console.WriteLine("=========================");
        Console.WriteLine("Arrow's columnar format benefits low-cardinality string columns.");
        Console.WriteLine();
        RunCardinalityAnalysis();
    }

    private static void RunTheoreticalAnalysis()
    {
        Console.WriteLine("For a class with 7 fields (int, 3x string refs, double, bool, DateTime):");
        Console.WriteLine();
        
        // Calculate theoretical sizes
        var objectHeaderSize = IntPtr.Size == 8 ? 16 : 8; // Method table + sync block
        var intSize = 4;
        var stringRefSize = IntPtr.Size; // Reference size
        var doubleSize = 8;
        var boolSize = 1;
        var dateTimeSize = 8;
        
        // Padding for alignment
        var classFieldsSize = intSize + (3 * stringRefSize) + doubleSize + boolSize + dateTimeSize;
        var classTotalSize = objectHeaderSize + classFieldsSize;
        // Round up to pointer alignment
        classTotalSize = (classTotalSize + IntPtr.Size - 1) / IntPtr.Size * IntPtr.Size;
        
        Console.WriteLine($"  Per-object overhead in List<T>:");
        Console.WriteLine($"    Object header:    {objectHeaderSize} bytes");
        Console.WriteLine($"    Fields:           ~{classFieldsSize} bytes");
        Console.WriteLine($"    Total per item:   ~{classTotalSize} bytes (+ string content)");
        Console.WriteLine();
        
        // Arrow format
        Console.WriteLine($"  Arrow columnar format (per item, amortized):");
        Console.WriteLine($"    int column:       4 bytes");
        Console.WriteLine($"    3x string cols:   ~variable (offset arrays + deduplicated data)");
        Console.WriteLine($"    double column:    8 bytes");
        Console.WriteLine($"    bool column:      1 bit (packed)");
        Console.WriteLine($"    DateTime column:  8 bytes (stored as int64 timestamp)");
        Console.WriteLine($"    Fixed overhead:   ~20.125 bytes/item + string data");
        Console.WriteLine();
        
        // Show scaling
        int[] itemCounts = [1_000, 10_000, 100_000, 1_000_000];
        var avgStringLen = 12; // "Category_XX"
        
        Console.WriteLine("  Estimated memory usage (assuming ~12 char average string length):");
        Console.WriteLine();
        Console.WriteLine("  +-------------+-----------------+-----------------+--------------+");
        Console.WriteLine("  |   Items     |   List<T> (MB)  |   Arrow (MB)    |   Savings    |");
        Console.WriteLine("  +-------------+-----------------+-----------------+--------------+");
        
        foreach (var count in itemCounts)
        {
            // List estimate: object overhead + string objects (header + length + chars)
            var stringObjectSize = objectHeaderSize + 4 + (avgStringLen * 2); // header + length + UTF16 chars
            var listPerItem = classTotalSize + (3 * stringObjectSize); // 3 string fields
            var listTotalMB = (count * listPerItem) / (1024.0 * 1024.0);
            
            // Arrow estimate: fixed-width columns + string data (stored once if repeated)
            // With 100 unique categories repeated across items:
            var uniqueStrings = Math.Min(count, 100);
            var arrowFixedPerItem = 4 + 8 + (1.0 / 8) + 8; // int + double + bool-bit + datetime
            var arrowStringOverhead = 4 * 3; // 3 offset arrays (4 bytes per item)
            var arrowStringData = uniqueStrings * avgStringLen * 3; // Actual string bytes (3 columns)
            var arrowTotalMB = (count * (arrowFixedPerItem + arrowStringOverhead) + arrowStringData) / (1024.0 * 1024.0);
            
            var savingsPercent = (1.0 - arrowTotalMB / listTotalMB) * 100;
            
            Console.WriteLine($"  | {count,11:N0} | {listTotalMB,15:F2} | {arrowTotalMB,15:F2} | {savingsPercent,10:F1}% |");
        }
        
        Console.WriteLine("  +-------------+-----------------+-----------------+--------------+");
        Console.WriteLine();
        Console.WriteLine("  * Arrow savings increase with more items due to string deduplication");
        Console.WriteLine("  * Actual results depend on string cardinality and content");
    }

    private static void WarmUp()
    {
        Console.WriteLine("Warming up...");
        
        // Create and dispose some collections to warm up JIT
        var warmupItems = GenerateItems(1000, stringCardinality: 100);
        var warmupList = warmupItems.ToList();
        using var warmupCollection = warmupItems.ToArrowCollection();
        
        GC.KeepAlive(warmupList);
        GC.KeepAlive(warmupCollection);
        
        ForceFullGC();
        Console.WriteLine("Warmup complete.");
        Console.WriteLine();
    }

    private static void RunEmpiricalAnalysis()
    {
        int[] sizes = [10_000, 100_000, 1_000_000];

        Console.WriteLine("+-------------+-----------------+------------------+------------------+");
        Console.WriteLine("|   Items     |  List<T> Managed | Arrow Managed    |  Arrow Native    |");
        Console.WriteLine("|             |     Heap (MB)    |   Wrapper (MB)   |  (estimated MB)  |");
        Console.WriteLine("+-------------+-----------------+------------------+------------------+");

        foreach (var size in sizes)
        {
            var (listManaged, arrowManaged, arrowNativeEstimate) = MeasureFootprint(size);
            
            Console.WriteLine($"| {size,11:N0} | {listManaged / (1024.0 * 1024.0),16:F3} | {arrowManaged / (1024.0 * 1024.0),16:F3} | {arrowNativeEstimate / (1024.0 * 1024.0),16:F3} |");
        }

        Console.WriteLine("+-------------+-----------------+------------------+------------------+");
        Console.WriteLine();
        Console.WriteLine("  * 'Arrow Managed' = .NET object overhead only (RecordBatch wrapper)");
        Console.WriteLine("  * 'Arrow Native' = Estimated based on Arrow buffer sizes");
    }

    private static (long listManaged, long arrowManaged, long arrowNativeEstimate) MeasureFootprint(int itemCount)
    {
        // Measure List<T> managed heap footprint - generate items within scope
        ForceFullGC();
        var beforeList = GC.GetTotalMemory(true);
        
        var list = GenerateItems(itemCount, stringCardinality: 100);
        
        ForceFullGC();
        var listManaged = GC.GetTotalMemory(true) - beforeList;
        
        GC.KeepAlive(list);
        list = null;
        ForceFullGC();
        
        // Measure ArrowCollection managed wrapper overhead - generate fresh items
        ForceFullGC();
        var beforeArrow = GC.GetTotalMemory(true);
        
        var arrowCollection = GenerateItemsEnumerable(itemCount, stringCardinality: 100).ToArrowCollection();
        
        ForceFullGC();
        var arrowManaged = GC.GetTotalMemory(true) - beforeArrow;
        
        // Estimate native memory based on Arrow format
        // int(4) + 3*string-offset(4) + double(8) + bool(1/8) + datetime(8) per item
        // Plus string data for unique values
        var fixedPerItem = 4 + (3 * 4) + 8 + 0.125 + 8;
        var uniqueStrings = 100; // Our test cardinality
        var avgStringLen = 12;
        var stringData = uniqueStrings * avgStringLen * 3;
        var arrowNativeEstimate = (long)(itemCount * fixedPerItem + stringData);
        
        GC.KeepAlive(arrowCollection);
        arrowCollection.Dispose();
        ForceFullGC();
        
        return (listManaged, arrowManaged, arrowNativeEstimate);
    }

    private static void RunCardinalityAnalysis()
    {
        const int itemCount = 100_000;
        int[] cardinalities = [10, 100, 1000, 10000, itemCount];
        
        // Calculate theoretical List<T> memory for this item count
        var objectHeaderSize = IntPtr.Size == 8 ? 16 : 8;
        var classTotalSize = 64; // Approximate per-object size
        var avgStringLen = 12;
        var stringObjectSize = objectHeaderSize + 4 + (avgStringLen * 2);
        var listPerItem = classTotalSize + (3 * stringObjectSize);
        var listTotalBytes = (long)(itemCount * listPerItem);
        var listMB = listTotalBytes / (1024.0 * 1024.0);
        
        Console.WriteLine($"Testing with {itemCount:N0} items, varying string uniqueness:");
        Console.WriteLine($"(List<T> theoretical memory: {listMB:F2} MB - constant regardless of cardinality)");
        Console.WriteLine();
        Console.WriteLine("+---------------+------------------+------------------+------------------+");
        Console.WriteLine("|  Unique Strs  |  List<T> (MB)    |  Arrow Est (MB)  |  Savings         |");
        Console.WriteLine("+---------------+------------------+------------------+------------------+");
        
        foreach (var cardinality in cardinalities)
        {
            // Estimate Arrow memory - this is where cardinality matters!
            var fixedPerItem = 4 + (3 * 4) + 8 + 0.125 + 8; // int + 3*string-offsets + double + bool-bit + datetime
            var stringData = cardinality * avgStringLen * 3; // String bytes stored once per unique value
            var arrowEstimate = (long)(itemCount * fixedPerItem + stringData);
            
            var arrowMB = arrowEstimate / (1024.0 * 1024.0);
            var savings = (1.0 - arrowMB / listMB) * 100;
            
            Console.WriteLine($"| {cardinality,13:N0} | {listMB,16:F2} | {arrowMB,16:F2} | {savings,14:F1}% |");
        }
        
        Console.WriteLine("+---------------+------------------+------------------+------------------+");
        Console.WriteLine();
        Console.WriteLine("  Key insight: Arrow's columnar format excels when:");
        Console.WriteLine("  - String columns have low cardinality (many repeated values)");
        Console.WriteLine("  - Data has many rows (amortizes metadata overhead)");
        Console.WriteLine("  - Fields are fixed-width primitives (int, double, bool, DateTime)");
        Console.WriteLine();
        Console.WriteLine("  Note: With 100% unique strings, Arrow still provides ~65% savings due to");
        Console.WriteLine("        elimination of per-object overhead and UTF-8 vs UTF-16 encoding.");
    }

    private static void ForceFullGC()
    {
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, blocking: true, compacting: true);
        GC.WaitForPendingFinalizers();
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, blocking: true, compacting: true);
    }

    private static List<MemoryTestItem> GenerateItems(int count, int stringCardinality)
    {
        var baseDate = DateTime.UtcNow;
        var categories = Enumerable.Range(0, stringCardinality).Select(i => $"Category_{i}").ToArray();
        
        return Enumerable.Range(0, count).Select(i => new MemoryTestItem
        {
            Id = i,
            Category1 = categories[i % categories.Length],
            Category2 = categories[(i + 33) % categories.Length],
            Category3 = categories[(i + 67) % categories.Length],
            Value = i * 1.5,
            IsActive = i % 2 == 0,
            CreatedAt = baseDate.AddSeconds(-i)
        }).ToList();
    }

    private static IEnumerable<MemoryTestItem> GenerateItemsEnumerable(int count, int stringCardinality)
    {
        var baseDate = DateTime.UtcNow;
        var categories = Enumerable.Range(0, stringCardinality).Select(i => $"Category_{i}").ToArray();
        
        for (int i = 0; i < count; i++)
        {
            yield return new MemoryTestItem
            {
                Id = i,
                Category1 = categories[i % categories.Length],
                Category2 = categories[(i + 33) % categories.Length],
                Category3 = categories[(i + 67) % categories.Length],
                Value = i * 1.5,
                IsActive = i % 2 == 0,
                CreatedAt = baseDate.AddSeconds(-i)
            };
        }
    }
}

/// <summary>
/// Test item for memory footprint analysis.
/// </summary>
[ArrowRecord]
public class MemoryTestItem
{
    [ArrowArray]
    public int Id { get; set; }
    
    [ArrowArray]
    public string Category1 { get; set; } = "";
    
    [ArrowArray]
    public string Category2 { get; set; } = "";
    
    [ArrowArray]
    public string Category3 { get; set; } = "";
    
    [ArrowArray]
    public double Value { get; set; }
    
    [ArrowArray]
    public bool IsActive { get; set; }
    
    [ArrowArray]
    public DateTime CreatedAt { get; set; }
}
