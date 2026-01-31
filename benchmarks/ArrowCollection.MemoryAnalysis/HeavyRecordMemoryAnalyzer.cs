using ArrowCollection;

namespace ArrowCollection.MemoryAnalysis;

/// <summary>
/// Analyzes memory footprint for the extreme scenario: 200-property record type with 1 million items.
/// Property breakdown:
/// - 10 string properties with low cardinality (100 distinct values each)
/// - 5 DateTime properties with high cardinality (timestamps)
/// - 62 int properties with high cardinality
/// - 62 double properties with high cardinality
/// - 61 decimal properties with high cardinality
/// </summary>
public static class HeavyRecordMemoryAnalyzer
{
    private const int StringCardinality = 100;
    private const int ItemCount = 1_000_000;

    public static void Run()
    {
        Console.WriteLine();
        Console.WriteLine("╔══════════════════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║           EXTREME SCENARIO: 200-PROPERTY RECORD MEMORY ANALYSIS              ║");
        Console.WriteLine("║  1 Million items with 200 properties each (strings, timestamps, numerics)    ║");
        Console.WriteLine("╚══════════════════════════════════════════════════════════════════════════════╝");
        Console.WriteLine();

        PrintScenarioDetails();
        RunTheoreticalAnalysis();

        Console.WriteLine();
        Console.WriteLine("EMPIRICAL MEMORY ANALYSIS");
        Console.WriteLine("═════════════════════════");
        Console.WriteLine();

        WarmUp();
        RunEmpiricalAnalysis();
    }

    private static void PrintScenarioDetails()
    {
        Console.WriteLine("SCENARIO DETAILS");
        Console.WriteLine("════════════════");
        Console.WriteLine();
        Console.WriteLine($"  Total items:          {ItemCount:N0}");
        Console.WriteLine($"  Properties per item:  200");
        Console.WriteLine();
        Console.WriteLine("  Property breakdown:");
        Console.WriteLine("    ├── 10 string properties    (low cardinality: 100 distinct values each)");
        Console.WriteLine("    ├── 5  DateTime properties  (high cardinality: unique timestamps)");
        Console.WriteLine("    ├── 62 int properties       (high cardinality)");
        Console.WriteLine("    ├── 62 double properties    (high cardinality)");
        Console.WriteLine("    └── 61 decimal properties   (high cardinality)");
        Console.WriteLine();
    }

    private static void RunTheoreticalAnalysis()
    {
        Console.WriteLine("THEORETICAL MEMORY ANALYSIS");
        Console.WriteLine("═══════════════════════════");
        Console.WriteLine();

        var objectHeaderSize = IntPtr.Size == 8 ? 16 : 8;
        var stringRefSize = IntPtr.Size;

        // List<T> memory calculation for HeavyBenchmarkItem
        // Object header + all fields
        var intFieldsSize = 62 * 4;           // 62 int fields × 4 bytes
        var doubleFieldsSize = 62 * 8;        // 62 double fields × 8 bytes
        var decimalFieldsSize = 61 * 16;      // 61 decimal fields × 16 bytes
        var dateTimeFieldsSize = 5 * 8;       // 5 DateTime fields × 8 bytes
        var stringRefsSize = 10 * stringRefSize; // 10 string references

        var objectFieldsSize = intFieldsSize + doubleFieldsSize + decimalFieldsSize + dateTimeFieldsSize + stringRefsSize;
        var objectTotalSize = objectHeaderSize + objectFieldsSize;
        // Round up to 8-byte alignment
        objectTotalSize = (objectTotalSize + 7) / 8 * 8;

        // String object overhead (for List<T>)
        var avgStringLen = 14; // "CategoryXX_XXX"
        var stringObjectSize = objectHeaderSize + 4 + (avgStringLen * 2); // header + length field + UTF-16 chars
        var uniqueStringsPerColumn = StringCardinality;
        var totalUniqueStrings = uniqueStringsPerColumn * 10; // 10 string columns
        var totalStringMemory = totalUniqueStrings * stringObjectSize;

        // Each item references strings, but strings are shared (interned-like behavior in our test)
        // So we count unique strings once, not per-item
        var listPerItemSize = objectTotalSize; // Object size (strings are shared)
        var listTotalBytes = (long)ItemCount * listPerItemSize + totalStringMemory;
        var listTotalMB = listTotalBytes / (1024.0 * 1024.0);
        var listTotalGB = listTotalBytes / (1024.0 * 1024.0 * 1024.0);

        Console.WriteLine("  List<HeavyBenchmarkItem> theoretical breakdown:");
        Console.WriteLine($"    Per-object overhead:");
        Console.WriteLine($"      Object header:        {objectHeaderSize} bytes");
        Console.WriteLine($"      62 int fields:        {intFieldsSize} bytes");
        Console.WriteLine($"      62 double fields:     {doubleFieldsSize} bytes");
        Console.WriteLine($"      61 decimal fields:    {decimalFieldsSize} bytes");
        Console.WriteLine($"      5 DateTime fields:    {dateTimeFieldsSize} bytes");
        Console.WriteLine($"      10 string refs:       {stringRefsSize} bytes");
        Console.WriteLine($"      ────────────────────────────────────────");
        Console.WriteLine($"      Total per object:     ~{objectTotalSize} bytes");
        Console.WriteLine();
        Console.WriteLine($"    String storage (shared across items):");
        Console.WriteLine($"      Unique strings:       {totalUniqueStrings:N0} ({StringCardinality} per column × 10 columns)");
        Console.WriteLine($"      Avg string size:      ~{stringObjectSize} bytes (header + {avgStringLen} UTF-16 chars)");
        Console.WriteLine($"      Total string memory:  ~{totalStringMemory / (1024.0 * 1024.0):F2} MB");
        Console.WriteLine();
        Console.WriteLine($"    Total List<T> estimate: ~{listTotalMB:F2} MB ({listTotalGB:F3} GB)");
        Console.WriteLine();

        // Arrow columnar format calculation
        // Fixed-width columns
        var arrowIntBytes = (long)ItemCount * 62 * 4;           // 62 int columns × 4 bytes
        var arrowDoubleBytes = (long)ItemCount * 62 * 8;        // 62 double columns × 8 bytes
        var arrowDecimalBytes = (long)ItemCount * 61 * 16;      // 61 decimal columns × 16 bytes (Decimal128)
        var arrowDateTimeBytes = (long)ItemCount * 5 * 8;       // 5 DateTime columns × 8 bytes (int64 timestamp)

        // String columns with dictionary encoding (Arrow uses dictionary encoding for repeated strings)
        // Offset array: 4 bytes per item per column
        var arrowStringOffsets = (long)ItemCount * 10 * 4;
        // String data: stored once per unique value, UTF-8 encoded
        var arrowStringData = (long)totalUniqueStrings * avgStringLen; // UTF-8 (1 byte per ASCII char)

        var arrowTotalBytes = arrowIntBytes + arrowDoubleBytes + arrowDecimalBytes + arrowDateTimeBytes + arrowStringOffsets + arrowStringData;
        var arrowTotalMB = arrowTotalBytes / (1024.0 * 1024.0);
        var arrowTotalGB = arrowTotalBytes / (1024.0 * 1024.0 * 1024.0);

        Console.WriteLine("  ArrowCollection<HeavyBenchmarkItem> theoretical breakdown:");
        Console.WriteLine($"    Fixed-width columns:");
        Console.WriteLine($"      62 int columns:       {arrowIntBytes / (1024.0 * 1024.0):F2} MB");
        Console.WriteLine($"      62 double columns:    {arrowDoubleBytes / (1024.0 * 1024.0):F2} MB");
        Console.WriteLine($"      61 decimal columns:   {arrowDecimalBytes / (1024.0 * 1024.0):F2} MB");
        Console.WriteLine($"      5 DateTime columns:   {arrowDateTimeBytes / (1024.0 * 1024.0):F2} MB");
        Console.WriteLine();
        Console.WriteLine($"    String columns (10 columns, dictionary encoded):");
        Console.WriteLine($"      Offset arrays:        {arrowStringOffsets / (1024.0 * 1024.0):F2} MB");
        Console.WriteLine($"      String data (UTF-8):  {arrowStringData / (1024.0):F2} KB (deduplicated)");
        Console.WriteLine();
        Console.WriteLine($"    Total Arrow estimate:   ~{arrowTotalMB:F2} MB ({arrowTotalGB:F3} GB)");
        Console.WriteLine();

        var savingsPercent = (1.0 - arrowTotalBytes / (double)listTotalBytes) * 100;
        var savingsGB = (listTotalGB - arrowTotalGB);

        Console.WriteLine("  ┌─────────────────────────────────────────────────────────────┐");
        Console.WriteLine($"  │  THEORETICAL SAVINGS: {savingsPercent:F1}% ({savingsGB:F3} GB)                        │");
        Console.WriteLine("  └─────────────────────────────────────────────────────────────┘");
        Console.WriteLine();
        Console.WriteLine("  Key observations:");
        Console.WriteLine("  • Arrow eliminates per-object overhead (16 bytes × 1M items = 15.3 MB)");
        Console.WriteLine("  • String deduplication: 1000 unique strings vs 10M string references");
        Console.WriteLine("  • UTF-8 encoding is more compact than UTF-16 for ASCII strings");
        Console.WriteLine("  • Decimal fields benefit from Arrow's Decimal128 native support");
    }

    private static void WarmUp()
    {
        Console.WriteLine("Warming up with smaller dataset...");

        var warmupItems = GenerateHeavyItems(1000);
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
        Console.WriteLine("Generating source data...");
        var sourceItems = GenerateHeavyItems(ItemCount);
        ForceFullGC();

        Console.WriteLine($"Source data generated: {ItemCount:N0} items");
        Console.WriteLine();

        // Measure List<T>
        Console.WriteLine("Measuring List<HeavyBenchmarkItem> memory footprint...");
        ForceFullGC();
        var beforeList = GC.GetTotalMemory(true);

        var list = sourceItems.ToList();

        ForceFullGC();
        var listMemory = GC.GetTotalMemory(true) - beforeList;
        var listMB = listMemory / (1024.0 * 1024.0);
        var listGB = listMemory / (1024.0 * 1024.0 * 1024.0);

        Console.WriteLine($"  List<T> managed heap: {listMB:F2} MB ({listGB:F4} GB)");

        // Keep list alive while measuring Arrow
        GC.KeepAlive(list);

        // Measure ArrowCollection
        Console.WriteLine("Measuring ArrowCollection<HeavyBenchmarkItem> memory footprint...");
        ForceFullGC();
        var beforeArrow = GC.GetTotalMemory(true);

        var arrowCollection = sourceItems.ToArrowCollection();

        ForceFullGC();
        var arrowManagedMemory = GC.GetTotalMemory(true) - beforeArrow;
        var arrowManagedMB = arrowManagedMemory / (1024.0 * 1024.0);

        // Estimate native memory based on Arrow format
        var arrowNativeEstimate = EstimateArrowNativeMemory();
        var arrowNativeMB = arrowNativeEstimate / (1024.0 * 1024.0);
        var arrowNativeGB = arrowNativeEstimate / (1024.0 * 1024.0 * 1024.0);

        Console.WriteLine($"  Arrow managed wrapper: {arrowManagedMB:F2} MB");
        Console.WriteLine($"  Arrow native (estimated): {arrowNativeMB:F2} MB ({arrowNativeGB:F4} GB)");
        Console.WriteLine();

        // Summary
        var totalArrowMB = arrowManagedMB + arrowNativeMB;
        var totalArrowGB = totalArrowMB / 1024.0;
        var savingsPercent = (1.0 - totalArrowMB / listMB) * 100;
        var savingsMB = listMB - totalArrowMB;

        Console.WriteLine("╔══════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║                        SUMMARY                                   ║");
        Console.WriteLine("╠══════════════════════════════════════════════════════════════════╣");
        Console.WriteLine($"║  List<HeavyBenchmarkItem>:       {listMB,10:F2} MB ({listGB:F4} GB)       ║");
        Console.WriteLine($"║  ArrowCollection (total):        {totalArrowMB,10:F2} MB ({totalArrowGB:F4} GB)       ║");
        Console.WriteLine("╠══════════════════════════════════════════════════════════════════╣");
        Console.WriteLine($"║  Memory savings:                 {savingsMB,10:F2} MB ({savingsPercent:F1}%)          ║");
        Console.WriteLine("╚══════════════════════════════════════════════════════════════════╝");
        Console.WriteLine();

        // Cleanup
        list = null;
        GC.KeepAlive(arrowCollection);
        arrowCollection.Dispose();
        ForceFullGC();
    }

    private static long EstimateArrowNativeMemory()
    {
        // Calculate based on Arrow columnar format
        var intBytes = (long)ItemCount * 62 * 4;
        var doubleBytes = (long)ItemCount * 62 * 8;
        var decimalBytes = (long)ItemCount * 61 * 16;
        var dateTimeBytes = (long)ItemCount * 5 * 8;
        var stringOffsets = (long)ItemCount * 10 * 4;
        var stringData = (long)StringCardinality * 10 * 14; // ~14 chars per string

        return intBytes + doubleBytes + decimalBytes + dateTimeBytes + stringOffsets + stringData;
    }

    private static List<HeavyMemoryTestItem> GenerateHeavyItems(int count)
    {
        var items = new List<HeavyMemoryTestItem>(count);
        var baseDate = DateTime.UtcNow;

        // Pre-generate string pools for low cardinality
        var stringPools = new string[10][];
        for (int pool = 0; pool < 10; pool++)
        {
            stringPools[pool] = Enumerable.Range(0, StringCardinality)
                .Select(i => $"Category{pool:D2}_{i:D3}")
                .ToArray();
        }

        for (int i = 0; i < count; i++)
        {
            items.Add(new HeavyMemoryTestItem
            {
                // Strings with low cardinality
                String01 = stringPools[0][i % StringCardinality],
                String02 = stringPools[1][i % StringCardinality],
                String03 = stringPools[2][i % StringCardinality],
                String04 = stringPools[3][i % StringCardinality],
                String05 = stringPools[4][i % StringCardinality],
                String06 = stringPools[5][i % StringCardinality],
                String07 = stringPools[6][i % StringCardinality],
                String08 = stringPools[7][i % StringCardinality],
                String09 = stringPools[8][i % StringCardinality],
                String10 = stringPools[9][i % StringCardinality],

                // DateTimes with high cardinality
                Timestamp01 = baseDate.AddMilliseconds(-i * 5),
                Timestamp02 = baseDate.AddMilliseconds(-i * 7),
                Timestamp03 = baseDate.AddMilliseconds(-i * 11),
                Timestamp04 = baseDate.AddMilliseconds(-i * 13),
                Timestamp05 = baseDate.AddMilliseconds(-i * 17),

                // Int fields
                Int001 = i, Int002 = i + 1, Int003 = i + 2, Int004 = i + 3, Int005 = i + 4,
                Int006 = i + 5, Int007 = i + 6, Int008 = i + 7, Int009 = i + 8, Int010 = i + 9,
                Int011 = i + 10, Int012 = i + 11, Int013 = i + 12, Int014 = i + 13, Int015 = i + 14,
                Int016 = i + 15, Int017 = i + 16, Int018 = i + 17, Int019 = i + 18, Int020 = i + 19,
                Int021 = i + 20, Int022 = i + 21, Int023 = i + 22, Int024 = i + 23, Int025 = i + 24,
                Int026 = i + 25, Int027 = i + 26, Int028 = i + 27, Int029 = i + 28, Int030 = i + 29,
                Int031 = i + 30, Int032 = i + 31, Int033 = i + 32, Int034 = i + 33, Int035 = i + 34,
                Int036 = i + 35, Int037 = i + 36, Int038 = i + 37, Int039 = i + 38, Int040 = i + 39,
                Int041 = i + 40, Int042 = i + 41, Int043 = i + 42, Int044 = i + 43, Int045 = i + 44,
                Int046 = i + 45, Int047 = i + 46, Int048 = i + 47, Int049 = i + 48, Int050 = i + 49,
                Int051 = i + 50, Int052 = i + 51, Int053 = i + 52, Int054 = i + 53, Int055 = i + 54,
                Int056 = i + 55, Int057 = i + 56, Int058 = i + 57, Int059 = i + 58, Int060 = i + 59,
                Int061 = i + 60, Int062 = i + 61,

                // Double fields
                Double001 = i * 0.001, Double002 = i * 0.002, Double003 = i * 0.003, Double004 = i * 0.004, Double005 = i * 0.005,
                Double006 = i * 0.006, Double007 = i * 0.007, Double008 = i * 0.008, Double009 = i * 0.009, Double010 = i * 0.010,
                Double011 = i * 0.011, Double012 = i * 0.012, Double013 = i * 0.013, Double014 = i * 0.014, Double015 = i * 0.015,
                Double016 = i * 0.016, Double017 = i * 0.017, Double018 = i * 0.018, Double019 = i * 0.019, Double020 = i * 0.020,
                Double021 = i * 0.021, Double022 = i * 0.022, Double023 = i * 0.023, Double024 = i * 0.024, Double025 = i * 0.025,
                Double026 = i * 0.026, Double027 = i * 0.027, Double028 = i * 0.028, Double029 = i * 0.029, Double030 = i * 0.030,
                Double031 = i * 0.031, Double032 = i * 0.032, Double033 = i * 0.033, Double034 = i * 0.034, Double035 = i * 0.035,
                Double036 = i * 0.036, Double037 = i * 0.037, Double038 = i * 0.038, Double039 = i * 0.039, Double040 = i * 0.040,
                Double041 = i * 0.041, Double042 = i * 0.042, Double043 = i * 0.043, Double044 = i * 0.044, Double045 = i * 0.045,
                Double046 = i * 0.046, Double047 = i * 0.047, Double048 = i * 0.048, Double049 = i * 0.049, Double050 = i * 0.050,
                Double051 = i * 0.051, Double052 = i * 0.052, Double053 = i * 0.053, Double054 = i * 0.054, Double055 = i * 0.055,
                Double056 = i * 0.056, Double057 = i * 0.057, Double058 = i * 0.058, Double059 = i * 0.059, Double060 = i * 0.060,
                Double061 = i * 0.061, Double062 = i * 0.062,

                // Decimal fields
                Decimal001 = i * 0.001m, Decimal002 = i * 0.002m, Decimal003 = i * 0.003m, Decimal004 = i * 0.004m, Decimal005 = i * 0.005m,
                Decimal006 = i * 0.006m, Decimal007 = i * 0.007m, Decimal008 = i * 0.008m, Decimal009 = i * 0.009m, Decimal010 = i * 0.010m,
                Decimal011 = i * 0.011m, Decimal012 = i * 0.012m, Decimal013 = i * 0.013m, Decimal014 = i * 0.014m, Decimal015 = i * 0.015m,
                Decimal016 = i * 0.016m, Decimal017 = i * 0.017m, Decimal018 = i * 0.018m, Decimal019 = i * 0.019m, Decimal020 = i * 0.020m,
                Decimal021 = i * 0.021m, Decimal022 = i * 0.022m, Decimal023 = i * 0.023m, Decimal024 = i * 0.024m, Decimal025 = i * 0.025m,
                Decimal026 = i * 0.026m, Decimal027 = i * 0.027m, Decimal028 = i * 0.028m, Decimal029 = i * 0.029m, Decimal030 = i * 0.030m,
                Decimal031 = i * 0.031m, Decimal032 = i * 0.032m, Decimal033 = i * 0.033m, Decimal034 = i * 0.034m, Decimal035 = i * 0.035m,
                Decimal036 = i * 0.036m, Decimal037 = i * 0.037m, Decimal038 = i * 0.038m, Decimal039 = i * 0.039m, Decimal040 = i * 0.040m,
                Decimal041 = i * 0.041m, Decimal042 = i * 0.042m, Decimal043 = i * 0.043m, Decimal044 = i * 0.044m, Decimal045 = i * 0.045m,
                Decimal046 = i * 0.046m, Decimal047 = i * 0.047m, Decimal048 = i * 0.048m, Decimal049 = i * 0.049m, Decimal050 = i * 0.050m,
                Decimal051 = i * 0.051m, Decimal052 = i * 0.052m, Decimal053 = i * 0.053m, Decimal054 = i * 0.054m, Decimal055 = i * 0.055m,
                Decimal056 = i * 0.056m, Decimal057 = i * 0.057m, Decimal058 = i * 0.058m, Decimal059 = i * 0.059m, Decimal060 = i * 0.060m,
                Decimal061 = i * 0.061m
            });
        }

        return items;
    }

    private static void ForceFullGC()
    {
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, blocking: true, compacting: true);
        GC.WaitForPendingFinalizers();
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, blocking: true, compacting: true);
    }
}

/// <summary>
/// Heavy test item with 200 properties for memory footprint analysis.
/// </summary>
[ArrowRecord]
public class HeavyMemoryTestItem
{
    #region String Properties (10)
    [ArrowArray] public string String01 { get; set; } = "";
    [ArrowArray] public string String02 { get; set; } = "";
    [ArrowArray] public string String03 { get; set; } = "";
    [ArrowArray] public string String04 { get; set; } = "";
    [ArrowArray] public string String05 { get; set; } = "";
    [ArrowArray] public string String06 { get; set; } = "";
    [ArrowArray] public string String07 { get; set; } = "";
    [ArrowArray] public string String08 { get; set; } = "";
    [ArrowArray] public string String09 { get; set; } = "";
    [ArrowArray] public string String10 { get; set; } = "";
    #endregion

    #region DateTime Properties (5)
    [ArrowArray] public DateTime Timestamp01 { get; set; }
    [ArrowArray] public DateTime Timestamp02 { get; set; }
    [ArrowArray] public DateTime Timestamp03 { get; set; }
    [ArrowArray] public DateTime Timestamp04 { get; set; }
    [ArrowArray] public DateTime Timestamp05 { get; set; }
    #endregion

    #region Int Properties (62)
    [ArrowArray] public int Int001 { get; set; }
    [ArrowArray] public int Int002 { get; set; }
    [ArrowArray] public int Int003 { get; set; }
    [ArrowArray] public int Int004 { get; set; }
    [ArrowArray] public int Int005 { get; set; }
    [ArrowArray] public int Int006 { get; set; }
    [ArrowArray] public int Int007 { get; set; }
    [ArrowArray] public int Int008 { get; set; }
    [ArrowArray] public int Int009 { get; set; }
    [ArrowArray] public int Int010 { get; set; }
    [ArrowArray] public int Int011 { get; set; }
    [ArrowArray] public int Int012 { get; set; }
    [ArrowArray] public int Int013 { get; set; }
    [ArrowArray] public int Int014 { get; set; }
    [ArrowArray] public int Int015 { get; set; }
    [ArrowArray] public int Int016 { get; set; }
    [ArrowArray] public int Int017 { get; set; }
    [ArrowArray] public int Int018 { get; set; }
    [ArrowArray] public int Int019 { get; set; }
    [ArrowArray] public int Int020 { get; set; }
    [ArrowArray] public int Int021 { get; set; }
    [ArrowArray] public int Int022 { get; set; }
    [ArrowArray] public int Int023 { get; set; }
    [ArrowArray] public int Int024 { get; set; }
    [ArrowArray] public int Int025 { get; set; }
    [ArrowArray] public int Int026 { get; set; }
    [ArrowArray] public int Int027 { get; set; }
    [ArrowArray] public int Int028 { get; set; }
    [ArrowArray] public int Int029 { get; set; }
    [ArrowArray] public int Int030 { get; set; }
    [ArrowArray] public int Int031 { get; set; }
    [ArrowArray] public int Int032 { get; set; }
    [ArrowArray] public int Int033 { get; set; }
    [ArrowArray] public int Int034 { get; set; }
    [ArrowArray] public int Int035 { get; set; }
    [ArrowArray] public int Int036 { get; set; }
    [ArrowArray] public int Int037 { get; set; }
    [ArrowArray] public int Int038 { get; set; }
    [ArrowArray] public int Int039 { get; set; }
    [ArrowArray] public int Int040 { get; set; }
    [ArrowArray] public int Int041 { get; set; }
    [ArrowArray] public int Int042 { get; set; }
    [ArrowArray] public int Int043 { get; set; }
    [ArrowArray] public int Int044 { get; set; }
    [ArrowArray] public int Int045 { get; set; }
    [ArrowArray] public int Int046 { get; set; }
    [ArrowArray] public int Int047 { get; set; }
    [ArrowArray] public int Int048 { get; set; }
    [ArrowArray] public int Int049 { get; set; }
    [ArrowArray] public int Int050 { get; set; }
    [ArrowArray] public int Int051 { get; set; }
    [ArrowArray] public int Int052 { get; set; }
    [ArrowArray] public int Int053 { get; set; }
    [ArrowArray] public int Int054 { get; set; }
    [ArrowArray] public int Int055 { get; set; }
    [ArrowArray] public int Int056 { get; set; }
    [ArrowArray] public int Int057 { get; set; }
    [ArrowArray] public int Int058 { get; set; }
    [ArrowArray] public int Int059 { get; set; }
    [ArrowArray] public int Int060 { get; set; }
    [ArrowArray] public int Int061 { get; set; }
    [ArrowArray] public int Int062 { get; set; }
    #endregion

    #region Double Properties (62)
    [ArrowArray] public double Double001 { get; set; }
    [ArrowArray] public double Double002 { get; set; }
    [ArrowArray] public double Double003 { get; set; }
    [ArrowArray] public double Double004 { get; set; }
    [ArrowArray] public double Double005 { get; set; }
    [ArrowArray] public double Double006 { get; set; }
    [ArrowArray] public double Double007 { get; set; }
    [ArrowArray] public double Double008 { get; set; }
    [ArrowArray] public double Double009 { get; set; }
    [ArrowArray] public double Double010 { get; set; }
    [ArrowArray] public double Double011 { get; set; }
    [ArrowArray] public double Double012 { get; set; }
    [ArrowArray] public double Double013 { get; set; }
    [ArrowArray] public double Double014 { get; set; }
    [ArrowArray] public double Double015 { get; set; }
    [ArrowArray] public double Double016 { get; set; }
    [ArrowArray] public double Double017 { get; set; }
    [ArrowArray] public double Double018 { get; set; }
    [ArrowArray] public double Double019 { get; set; }
    [ArrowArray] public double Double020 { get; set; }
    [ArrowArray] public double Double021 { get; set; }
    [ArrowArray] public double Double022 { get; set; }
    [ArrowArray] public double Double023 { get; set; }
    [ArrowArray] public double Double024 { get; set; }
    [ArrowArray] public double Double025 { get; set; }
    [ArrowArray] public double Double026 { get; set; }
    [ArrowArray] public double Double027 { get; set; }
    [ArrowArray] public double Double028 { get; set; }
    [ArrowArray] public double Double029 { get; set; }
    [ArrowArray] public double Double030 { get; set; }
    [ArrowArray] public double Double031 { get; set; }
    [ArrowArray] public double Double032 { get; set; }
    [ArrowArray] public double Double033 { get; set; }
    [ArrowArray] public double Double034 { get; set; }
    [ArrowArray] public double Double035 { get; set; }
    [ArrowArray] public double Double036 { get; set; }
    [ArrowArray] public double Double037 { get; set; }
    [ArrowArray] public double Double038 { get; set; }
    [ArrowArray] public double Double039 { get; set; }
    [ArrowArray] public double Double040 { get; set; }
    [ArrowArray] public double Double041 { get; set; }
    [ArrowArray] public double Double042 { get; set; }
    [ArrowArray] public double Double043 { get; set; }
    [ArrowArray] public double Double044 { get; set; }
    [ArrowArray] public double Double045 { get; set; }
    [ArrowArray] public double Double046 { get; set; }
    [ArrowArray] public double Double047 { get; set; }
    [ArrowArray] public double Double048 { get; set; }
    [ArrowArray] public double Double049 { get; set; }
    [ArrowArray] public double Double050 { get; set; }
    [ArrowArray] public double Double051 { get; set; }
    [ArrowArray] public double Double052 { get; set; }
    [ArrowArray] public double Double053 { get; set; }
    [ArrowArray] public double Double054 { get; set; }
    [ArrowArray] public double Double055 { get; set; }
    [ArrowArray] public double Double056 { get; set; }
    [ArrowArray] public double Double057 { get; set; }
    [ArrowArray] public double Double058 { get; set; }
    [ArrowArray] public double Double059 { get; set; }
    [ArrowArray] public double Double060 { get; set; }
    [ArrowArray] public double Double061 { get; set; }
    [ArrowArray] public double Double062 { get; set; }
    #endregion

    #region Decimal Properties (61)
    [ArrowArray] public decimal Decimal001 { get; set; }
    [ArrowArray] public decimal Decimal002 { get; set; }
    [ArrowArray] public decimal Decimal003 { get; set; }
    [ArrowArray] public decimal Decimal004 { get; set; }
    [ArrowArray] public decimal Decimal005 { get; set; }
    [ArrowArray] public decimal Decimal006 { get; set; }
    [ArrowArray] public decimal Decimal007 { get; set; }
    [ArrowArray] public decimal Decimal008 { get; set; }
    [ArrowArray] public decimal Decimal009 { get; set; }
    [ArrowArray] public decimal Decimal010 { get; set; }
    [ArrowArray] public decimal Decimal011 { get; set; }
    [ArrowArray] public decimal Decimal012 { get; set; }
    [ArrowArray] public decimal Decimal013 { get; set; }
    [ArrowArray] public decimal Decimal014 { get; set; }
    [ArrowArray] public decimal Decimal015 { get; set; }
    [ArrowArray] public decimal Decimal016 { get; set; }
    [ArrowArray] public decimal Decimal017 { get; set; }
    [ArrowArray] public decimal Decimal018 { get; set; }
    [ArrowArray] public decimal Decimal019 { get; set; }
    [ArrowArray] public decimal Decimal020 { get; set; }
    [ArrowArray] public decimal Decimal021 { get; set; }
    [ArrowArray] public decimal Decimal022 { get; set; }
    [ArrowArray] public decimal Decimal023 { get; set; }
    [ArrowArray] public decimal Decimal024 { get; set; }
    [ArrowArray] public decimal Decimal025 { get; set; }
    [ArrowArray] public decimal Decimal026 { get; set; }
    [ArrowArray] public decimal Decimal027 { get; set; }
    [ArrowArray] public decimal Decimal028 { get; set; }
    [ArrowArray] public decimal Decimal029 { get; set; }
    [ArrowArray] public decimal Decimal030 { get; set; }
    [ArrowArray] public decimal Decimal031 { get; set; }
    [ArrowArray] public decimal Decimal032 { get; set; }
    [ArrowArray] public decimal Decimal033 { get; set; }
    [ArrowArray] public decimal Decimal034 { get; set; }
    [ArrowArray] public decimal Decimal035 { get; set; }
    [ArrowArray] public decimal Decimal036 { get; set; }
    [ArrowArray] public decimal Decimal037 { get; set; }
    [ArrowArray] public decimal Decimal038 { get; set; }
    [ArrowArray] public decimal Decimal039 { get; set; }
    [ArrowArray] public decimal Decimal040 { get; set; }
    [ArrowArray] public decimal Decimal041 { get; set; }
    [ArrowArray] public decimal Decimal042 { get; set; }
    [ArrowArray] public decimal Decimal043 { get; set; }
    [ArrowArray] public decimal Decimal044 { get; set; }
    [ArrowArray] public decimal Decimal045 { get; set; }
    [ArrowArray] public decimal Decimal046 { get; set; }
    [ArrowArray] public decimal Decimal047 { get; set; }
    [ArrowArray] public decimal Decimal048 { get; set; }
    [ArrowArray] public decimal Decimal049 { get; set; }
    [ArrowArray] public decimal Decimal050 { get; set; }
    [ArrowArray] public decimal Decimal051 { get; set; }
    [ArrowArray] public decimal Decimal052 { get; set; }
    [ArrowArray] public decimal Decimal053 { get; set; }
    [ArrowArray] public decimal Decimal054 { get; set; }
    [ArrowArray] public decimal Decimal055 { get; set; }
    [ArrowArray] public decimal Decimal056 { get; set; }
    [ArrowArray] public decimal Decimal057 { get; set; }
    [ArrowArray] public decimal Decimal058 { get; set; }
    [ArrowArray] public decimal Decimal059 { get; set; }
    [ArrowArray] public decimal Decimal060 { get; set; }
    [ArrowArray] public decimal Decimal061 { get; set; }
    #endregion
}
