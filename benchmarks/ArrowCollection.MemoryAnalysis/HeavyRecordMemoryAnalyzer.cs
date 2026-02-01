using System.Diagnostics;
using ArrowCollection;

namespace ArrowCollection.MemoryAnalysis;

/// <summary>
/// Analyzes memory footprint for the extreme scenario: 200-property record type with 1 million items.
/// Property breakdown:
/// - 10 string properties with low cardinality (100 distinct values each)
/// - 5 DateTime properties with high cardinality (timestamps)
/// - 62 int properties (block-based sparse: each row populates one block of 10)
/// - 62 double properties (block-based sparse: each row populates one block of 10)
/// - 61 decimal properties (block-based sparse: each row populates one block of 10)
/// 
/// This simulates a dataset that has combined data from multiple sources, where each source
/// only populated its specific subset of columns. Rows are evenly distributed across 7 blocks,
/// with each block containing ~10 properties that have values (rest are default/zero).
/// </summary>
public static class HeavyRecordMemoryAnalyzer
{
    private const int StringCardinality = 100;
    private const int ItemCount = 1_000_000;
    private const int BlockSize = 10;
    private const int NumBlocks = 7; // ceil(62/10) = 7 blocks for int/double, ceil(61/10) = 7 for decimal

    public static void Run()
    {
        Console.WriteLine();
        Console.WriteLine("+==============================================================================+");
        Console.WriteLine("|           EXTREME SCENARIO: 200-PROPERTY RECORD MEMORY ANALYSIS              |");
        Console.WriteLine("|  1 Million items with 200 properties each (sparse wide dataset)              |");
        Console.WriteLine("+==============================================================================+");
        Console.WriteLine();

        PrintScenarioDetails();
        RunTheoreticalAnalysis();

        Console.WriteLine();
        Console.WriteLine("EMPIRICAL MEMORY ANALYSIS");
        Console.WriteLine("=========================");
        Console.WriteLine();

        WarmUp();
        
        // Scenario 1: Random order (chaotic production data)
        RunEmpiricalAnalysis("SCENARIO 1: RANDOM ORDER (Chaotic Production Data)", usePreSorting: false);
        
        // Scenario 2: Pre-sorted by cardinality (column store optimization)
        RunEmpiricalAnalysis("SCENARIO 2: PRE-SORTED BY CARDINALITY (Column Store Optimization)", usePreSorting: true);
    }

    private static void PrintScenarioDetails()
    {
        Console.WriteLine("SCENARIO DETAILS");
        Console.WriteLine("================");
        Console.WriteLine();
        Console.WriteLine($"  Total items:          {ItemCount:N0}");
        Console.WriteLine($"  Properties per item:  200");
        Console.WriteLine($"  Block size:           {BlockSize} properties");
        Console.WriteLine($"  Number of blocks:     {NumBlocks}");
        Console.WriteLine();
        Console.WriteLine("  Property breakdown (combined dataset simulation):");
        Console.WriteLine("    +-- 10 string properties    (low cardinality: 100 distinct values each)");
        Console.WriteLine("    +-- 5  DateTime properties  (high cardinality: unique timestamps)");
        Console.WriteLine("    +-- 62 int properties       (7 blocks of ~10, one block active per row)");
        Console.WriteLine("    +-- 62 double properties    (7 blocks of ~10, one block active per row)");
        Console.WriteLine("    +-- 61 decimal properties   (7 blocks of ~10, one block active per row)");
        Console.WriteLine();
        Console.WriteLine("  This simulates a combined dataset where multiple data sources have been merged,");
        Console.WriteLine("  each source populating only its specific subset of columns. Rows are evenly");
        Console.WriteLine("  distributed across blocks (rowIndex % 7 determines active block).");
        Console.WriteLine();
    }

    private static void RunTheoreticalAnalysis()
    {
        Console.WriteLine("THEORETICAL MEMORY ANALYSIS");
        Console.WriteLine("===========================");
        Console.WriteLine();

        var objectHeaderSize = IntPtr.Size == 8 ? 16 : 8;
        var stringRefSize = IntPtr.Size;

        // List<T> memory calculation - same regardless of sparse values
        var intFieldsSize = 62 * 4;
        var doubleFieldsSize = 62 * 8;
        var decimalFieldsSize = 61 * 16;
        var dateTimeFieldsSize = 5 * 8;
        var stringRefsSize = 10 * stringRefSize;

        var objectFieldsSize = intFieldsSize + doubleFieldsSize + decimalFieldsSize + dateTimeFieldsSize + stringRefsSize;
        var objectTotalSize = objectHeaderSize + objectFieldsSize;
        objectTotalSize = (objectTotalSize + 7) / 8 * 8;

        var avgStringLen = 14;
        var stringObjectSize = objectHeaderSize + 4 + (avgStringLen * 2);
        var totalUniqueStrings = StringCardinality * 10;
        var totalStringMemory = totalUniqueStrings * stringObjectSize;

        var listTotalBytes = (long)ItemCount * objectTotalSize + totalStringMemory;
        var listTotalMB = listTotalBytes / (1024.0 * 1024.0);
        var listTotalGB = listTotalBytes / (1024.0 * 1024.0 * 1024.0);

        Console.WriteLine("  List<T> theoretical breakdown:");
        Console.WriteLine($"    Total per object:     ~{objectTotalSize} bytes (same regardless of sparse values)");
        Console.WriteLine($"    Total List<T> estimate: ~{listTotalMB:F2} MB ({listTotalGB:F3} GB)");
        Console.WriteLine();

        // Arrow columnar format - values are stored regardless of being zero
        var arrowIntBytes = (long)ItemCount * 62 * 4;
        var arrowDoubleBytes = (long)ItemCount * 62 * 8;
        var arrowDecimalBytes = (long)ItemCount * 61 * 16;
        var arrowDateTimeBytes = (long)ItemCount * 5 * 8;
        var arrowStringOffsets = (long)ItemCount * 10 * 4;
        var arrowStringData = (long)totalUniqueStrings * avgStringLen;

        var arrowTotalBytes = arrowIntBytes + arrowDoubleBytes + arrowDecimalBytes + arrowDateTimeBytes + arrowStringOffsets + arrowStringData;
        var arrowTotalMB = arrowTotalBytes / (1024.0 * 1024.0);
        var arrowTotalGB = arrowTotalBytes / (1024.0 * 1024.0 * 1024.0);

        Console.WriteLine("  Arrow theoretical breakdown:");
        Console.WriteLine($"    Total Arrow estimate:   ~{arrowTotalMB:F2} MB ({arrowTotalGB:F3} GB)");
        Console.WriteLine();

        var savingsPercent = (1.0 - arrowTotalBytes / (double)listTotalBytes) * 100;
        Console.WriteLine($"  Theoretical savings: {savingsPercent:F1}%");
        Console.WriteLine();
        Console.WriteLine("  Note: Arrow stores all values including zeros. Sparse data patterns may");
        Console.WriteLine("        benefit from dictionary encoding or run-length encoding in some cases.");
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


    private static void RunEmpiricalAnalysis(string scenarioTitle, bool usePreSorting)
    {
        Console.WriteLine($"---------------------------------------------------------------------");
        Console.WriteLine(scenarioTitle);
        Console.WriteLine($"---------------------------------------------------------------------");
        Console.WriteLine();

        var process = Process.GetCurrentProcess();

        // Measure List<T> using process memory
        Console.WriteLine("Measuring List<T> memory footprint...");
        Console.WriteLine("  (Generating 1M sparse items and storing in List<T>...)");
        ForceFullGC();
        Thread.Sleep(100);
        process.Refresh();
        var beforeList = process.PrivateMemorySize64;

        var list = GenerateHeavyItems(ItemCount);

        ForceFullGC();
        Thread.Sleep(100);
        process.Refresh();
        var listMemory = process.PrivateMemorySize64 - beforeList;
        var listMB = listMemory / (1024.0 * 1024.0);
        var listGB = listMemory / (1024.0 * 1024.0 * 1024.0);

        Console.WriteLine($"  List<T> process memory: {listMB:F2} MB ({listGB:F4} GB)");
        Console.WriteLine();

        GC.KeepAlive(list);
        list = null;
        ForceFullGC();
        Thread.Sleep(200);

        // Measure ArrowCollection using process memory
        if (usePreSorting)
        {
            Console.WriteLine("Measuring ArrowCollection memory footprint (PRE-SORTED by cardinality)...");
            Console.WriteLine("  (Sorting: String columns first -> Sparse columns -> High-cardinality columns)");
        }
        else
        {
            Console.WriteLine("Measuring ArrowCollection memory footprint (RANDOM order)...");
        }
        Console.WriteLine("  (Generating 1M sparse items and converting to ArrowCollection...)");
        
        ForceFullGC();
        Thread.Sleep(100);
        process.Refresh();
        var beforeArrow = process.PrivateMemorySize64;

        ArrowCollection<HeavyMemoryTestItem> arrowCollection;
        if (usePreSorting)
        {
            // Pre-sort by cardinality: lowest first (strings), then sparse numerics, then high-cardinality
            arrowCollection = GenerateHeavyItemsEnumerable(ItemCount)
                .OrderBy(x => x.String01)
                .ThenBy(x => x.String02)
                .ThenBy(x => x.String03)
                .ThenBy(x => x.String04)
                .ThenBy(x => x.String05)
                .ThenBy(x => x.String06)
                .ThenBy(x => x.String07)
                .ThenBy(x => x.String08)
                .ThenBy(x => x.String09)
                .ThenBy(x => x.String10)
                .ThenBy(x => x.Int001)
                .ThenBy(x => x.Double001)
                .ThenBy(x => x.Decimal001)
                .ToArrowCollection();
        }
        else
        {
            arrowCollection = GenerateHeavyItemsEnumerable(ItemCount).ToArrowCollection();
        }

        ForceFullGC();
        Thread.Sleep(100);
        process.Refresh();
        var arrowMemory = process.PrivateMemorySize64 - beforeArrow;
        var arrowMB = arrowMemory / (1024.0 * 1024.0);
        var arrowGB = arrowMemory / (1024.0 * 1024.0 * 1024.0);

        Console.WriteLine($"  ArrowCollection process memory: {arrowMB:F2} MB ({arrowGB:F4} GB)");
        Console.WriteLine();

        // Display build statistics
        if (arrowCollection.BuildStatistics is not null)
        {
            Console.WriteLine("BUILD STATISTICS:");
            Console.WriteLine("-----------------");
            var stats = arrowCollection.BuildStatistics;
            Console.WriteLine($"  Statistics collection time: {stats.StatisticsCollectionTime?.TotalMilliseconds:F2}ms");
            Console.WriteLine($"  Columns using dictionary encoding: {stats.GetDictionaryEncodingCandidates().Count()}");
            Console.WriteLine($"  Columns using RLE encoding: {stats.GetRunLengthEncodingCandidates().Count()}");
            Console.WriteLine();
            
            // Show dictionary encoding candidates
            var dictCandidates = stats.GetDictionaryEncodingCandidates().ToList();
            Console.WriteLine($"  Dictionary Encoding Candidates ({dictCandidates.Count} columns):");
            foreach (var col in dictCandidates.Take(10))
            {
                Console.WriteLine($"    - {col.ColumnName}: {col.DistinctCount:N0} distinct / {col.TotalCount:N0} total ({col.CardinalityRatio:P1})");
            }
            if (dictCandidates.Count > 10)
                Console.WriteLine($"    ... and {dictCandidates.Count - 10} more");
            Console.WriteLine();
            
            // Show RLE candidates
            var rleCandidates = stats.GetRunLengthEncodingCandidates().ToList();
            Console.WriteLine($"  Run-Length Encoding Candidates ({rleCandidates.Count} columns):");
            foreach (var col in rleCandidates.Take(10))
            {
                Console.WriteLine($"    - {col.ColumnName}: {col.RunCount:N0} runs / {col.TotalCount:N0} total ({col.RunRatio:P1})");
            }
            if (rleCandidates.Count > 10)
                Console.WriteLine($"    ... and {rleCandidates.Count - 10} more");
            Console.WriteLine();
        }

        // Calculate savings
        var savingsPercent = listMemory > 0 ? (1.0 - (double)arrowMemory / listMemory) * 100 : 0;
        var savingsMB = listMB - arrowMB;

        Console.WriteLine("+==================================================================+");
        Console.WriteLine($"|  {(usePreSorting ? "PRE-SORTED" : "RANDOM ORDER"),-15} SUMMARY (Process Memory)                  |");
        Console.WriteLine("+==================================================================+");
        Console.WriteLine($"|  List<T>:                {listMB,10:F2} MB ({listGB:F4} GB)       |");
        Console.WriteLine($"|  ArrowCollection:        {arrowMB,10:F2} MB ({arrowGB:F4} GB)       |");
        Console.WriteLine("+------------------------------------------------------------------+");
        Console.WriteLine($"|  Memory savings:         {savingsMB,10:F2} MB ({savingsPercent:F1}%)          |");
        Console.WriteLine("+==================================================================+");
        Console.WriteLine();

        GC.KeepAlive(arrowCollection);
        arrowCollection.Dispose();
        ForceFullGC();
        Thread.Sleep(200);
    }

    /// <summary>
    /// Generates sparse heavy items using block-based property population.
    /// Each row belongs to one block (determined by rowIndex % NumBlocks), and only
    /// properties in that block have values. This simulates combined datasets.
    /// </summary>
    private static List<HeavyMemoryTestItem> GenerateHeavyItems(int count)
    {
        var items = new List<HeavyMemoryTestItem>(count);
        var baseDate = DateTime.UtcNow;

        var stringPools = new string[10][];
        for (int pool = 0; pool < 10; pool++)
        {
            stringPools[pool] = Enumerable.Range(0, StringCardinality)
                .Select(i => $"Category{pool:D2}_{i:D3}")
                .ToArray();
        }

        for (int i = 0; i < count; i++)
        {
            // Determine which block this row belongs to
            var blockIndex = i % NumBlocks;

            var item = new HeavyMemoryTestItem
            {
                // Strings with low cardinality (always populated)
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

                // DateTimes with high cardinality (always populated)
                Timestamp01 = baseDate.AddMilliseconds(-i * 5),
                Timestamp02 = baseDate.AddMilliseconds(-i * 7),
                Timestamp03 = baseDate.AddMilliseconds(-i * 11),
                Timestamp04 = baseDate.AddMilliseconds(-i * 13),
                Timestamp05 = baseDate.AddMilliseconds(-i * 17),
            };

            // Set only the properties in this row's block
            SetBlockIntValues(item, i, blockIndex);
            SetBlockDoubleValues(item, i, blockIndex);
            SetBlockDecimalValues(item, i, blockIndex);

            items.Add(item);
        }

        return items;
    }

    private static IEnumerable<HeavyMemoryTestItem> GenerateHeavyItemsEnumerable(int count)
    {
        var baseDate = DateTime.UtcNow;

        var stringPools = new string[10][];
        for (int pool = 0; pool < 10; pool++)
        {
            stringPools[pool] = Enumerable.Range(0, StringCardinality)
                .Select(i => $"Category{pool:D2}_{i:D3}")
                .ToArray();
        }

        for (int i = 0; i < count; i++)
        {
            // Determine which block this row belongs to
            var blockIndex = i % NumBlocks;

            var item = new HeavyMemoryTestItem
            {
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

                Timestamp01 = baseDate.AddMilliseconds(-i * 5),
                Timestamp02 = baseDate.AddMilliseconds(-i * 7),
                Timestamp03 = baseDate.AddMilliseconds(-i * 11),
                Timestamp04 = baseDate.AddMilliseconds(-i * 13),
                Timestamp05 = baseDate.AddMilliseconds(-i * 17),
            };

            SetBlockIntValues(item, i, blockIndex);
            SetBlockDoubleValues(item, i, blockIndex);
            SetBlockDecimalValues(item, i, blockIndex);

            yield return item;
        }
    }

    /// <summary>
    /// Sets int property values for the specified block.
    /// Block 0: Int001-Int010, Block 1: Int011-Int020, etc.
    /// </summary>
    private static void SetBlockIntValues(HeavyMemoryTestItem item, int rowIndex, int blockIndex)
    {
        switch (blockIndex)
        {
            case 0: // Int001-Int010
                item.Int001 = rowIndex;
                item.Int002 = rowIndex + 1;
                item.Int003 = rowIndex + 2;
                item.Int004 = rowIndex + 3;
                item.Int005 = rowIndex + 4;
                item.Int006 = rowIndex + 5;
                item.Int007 = rowIndex + 6;
                item.Int008 = rowIndex + 7;
                item.Int009 = rowIndex + 8;
                item.Int010 = rowIndex + 9;
                break;
            case 1: // Int011-Int020
                item.Int011 = rowIndex + 10;
                item.Int012 = rowIndex + 11;
                item.Int013 = rowIndex + 12;
                item.Int014 = rowIndex + 13;
                item.Int015 = rowIndex + 14;
                item.Int016 = rowIndex + 15;
                item.Int017 = rowIndex + 16;
                item.Int018 = rowIndex + 17;
                item.Int019 = rowIndex + 18;
                item.Int020 = rowIndex + 19;
                break;
            case 2: // Int021-Int030
                item.Int021 = rowIndex + 20;
                item.Int022 = rowIndex + 21;
                item.Int023 = rowIndex + 22;
                item.Int024 = rowIndex + 23;
                item.Int025 = rowIndex + 24;
                item.Int026 = rowIndex + 25;
                item.Int027 = rowIndex + 26;
                item.Int028 = rowIndex + 27;
                item.Int029 = rowIndex + 28;
                item.Int030 = rowIndex + 29;
                break;
            case 3: // Int031-Int040
                item.Int031 = rowIndex + 30;
                item.Int032 = rowIndex + 31;
                item.Int033 = rowIndex + 32;
                item.Int034 = rowIndex + 33;
                item.Int035 = rowIndex + 34;
                item.Int036 = rowIndex + 35;
                item.Int037 = rowIndex + 36;
                item.Int038 = rowIndex + 37;
                item.Int039 = rowIndex + 38;
                item.Int040 = rowIndex + 39;
                break;
            case 4: // Int041-Int050
                item.Int041 = rowIndex + 40;
                item.Int042 = rowIndex + 41;
                item.Int043 = rowIndex + 42;
                item.Int044 = rowIndex + 43;
                item.Int045 = rowIndex + 44;
                item.Int046 = rowIndex + 45;
                item.Int047 = rowIndex + 46;
                item.Int048 = rowIndex + 47;
                item.Int049 = rowIndex + 48;
                item.Int050 = rowIndex + 49;
                break;
            case 5: // Int051-Int060
                item.Int051 = rowIndex + 50;
                item.Int052 = rowIndex + 51;
                item.Int053 = rowIndex + 52;
                item.Int054 = rowIndex + 53;
                item.Int055 = rowIndex + 54;
                item.Int056 = rowIndex + 55;
                item.Int057 = rowIndex + 56;
                item.Int058 = rowIndex + 57;
                item.Int059 = rowIndex + 58;
                item.Int060 = rowIndex + 59;
                break;
            case 6: // Int061-Int062 (incomplete block)
                item.Int061 = rowIndex + 60;
                item.Int062 = rowIndex + 61;
                break;
        }
    }

    /// <summary>
    /// Sets double property values for the specified block.
    /// </summary>
    private static void SetBlockDoubleValues(HeavyMemoryTestItem item, int rowIndex, int blockIndex)
    {
        switch (blockIndex)
        {
            case 0:
                item.Double001 = rowIndex * 0.001;
                item.Double002 = rowIndex * 0.002;
                item.Double003 = rowIndex * 0.003;
                item.Double004 = rowIndex * 0.004;
                item.Double005 = rowIndex * 0.005;
                item.Double006 = rowIndex * 0.006;
                item.Double007 = rowIndex * 0.007;
                item.Double008 = rowIndex * 0.008;
                item.Double009 = rowIndex * 0.009;
                item.Double010 = rowIndex * 0.010;
                break;
            case 1:
                item.Double011 = rowIndex * 0.011;
                item.Double012 = rowIndex * 0.012;
                item.Double013 = rowIndex * 0.013;
                item.Double014 = rowIndex * 0.014;
                item.Double015 = rowIndex * 0.015;
                item.Double016 = rowIndex * 0.016;
                item.Double017 = rowIndex * 0.017;
                item.Double018 = rowIndex * 0.018;
                item.Double019 = rowIndex * 0.019;
                item.Double020 = rowIndex * 0.020;
                break;
            case 2:
                item.Double021 = rowIndex * 0.021;
                item.Double022 = rowIndex * 0.022;
                item.Double023 = rowIndex * 0.023;
                item.Double024 = rowIndex * 0.024;
                item.Double025 = rowIndex * 0.025;
                item.Double026 = rowIndex * 0.026;
                item.Double027 = rowIndex * 0.027;
                item.Double028 = rowIndex * 0.028;
                item.Double029 = rowIndex * 0.029;
                item.Double030 = rowIndex * 0.030;
                break;
            case 3:
                item.Double031 = rowIndex * 0.031;
                item.Double032 = rowIndex * 0.032;
                item.Double033 = rowIndex * 0.033;
                item.Double034 = rowIndex * 0.034;
                item.Double035 = rowIndex * 0.035;
                item.Double036 = rowIndex * 0.036;
                item.Double037 = rowIndex * 0.037;
                item.Double038 = rowIndex * 0.038;
                item.Double039 = rowIndex * 0.039;
                item.Double040 = rowIndex * 0.040;
                break;
            case 4:
                item.Double041 = rowIndex * 0.041;
                item.Double042 = rowIndex * 0.042;
                item.Double043 = rowIndex * 0.043;
                item.Double044 = rowIndex * 0.044;
                item.Double045 = rowIndex * 0.045;
                item.Double046 = rowIndex * 0.046;
                item.Double047 = rowIndex * 0.047;
                item.Double048 = rowIndex * 0.048;
                item.Double049 = rowIndex * 0.049;
                item.Double050 = rowIndex * 0.050;
                break;
            case 5:
                item.Double051 = rowIndex * 0.051;
                item.Double052 = rowIndex * 0.052;
                item.Double053 = rowIndex * 0.053;
                item.Double054 = rowIndex * 0.054;
                item.Double055 = rowIndex * 0.055;
                item.Double056 = rowIndex * 0.056;
                item.Double057 = rowIndex * 0.057;
                item.Double058 = rowIndex * 0.058;
                item.Double059 = rowIndex * 0.059;
                item.Double060 = rowIndex * 0.060;
                break;
            case 6:
                item.Double061 = rowIndex * 0.061;
                item.Double062 = rowIndex * 0.062;
                break;
        }
    }

    /// <summary>
    /// Sets decimal property values for the specified block.
    /// </summary>
    private static void SetBlockDecimalValues(HeavyMemoryTestItem item, int rowIndex, int blockIndex)
    {
        switch (blockIndex)
        {
            case 0:
                item.Decimal001 = rowIndex * 0.001m;
                item.Decimal002 = rowIndex * 0.002m;
                item.Decimal003 = rowIndex * 0.003m;
                item.Decimal004 = rowIndex * 0.004m;
                item.Decimal005 = rowIndex * 0.005m;
                item.Decimal006 = rowIndex * 0.006m;
                item.Decimal007 = rowIndex * 0.007m;
                item.Decimal008 = rowIndex * 0.008m;
                item.Decimal009 = rowIndex * 0.009m;
                item.Decimal010 = rowIndex * 0.010m;
                break;
            case 1:
                item.Decimal011 = rowIndex * 0.011m;
                item.Decimal012 = rowIndex * 0.012m;
                item.Decimal013 = rowIndex * 0.013m;
                item.Decimal014 = rowIndex * 0.014m;
                item.Decimal015 = rowIndex * 0.015m;
                item.Decimal016 = rowIndex * 0.016m;
                item.Decimal017 = rowIndex * 0.017m;
                item.Decimal018 = rowIndex * 0.018m;
                item.Decimal019 = rowIndex * 0.019m;
                item.Decimal020 = rowIndex * 0.020m;
                break;
            case 2:
                item.Decimal021 = rowIndex * 0.021m;
                item.Decimal022 = rowIndex * 0.022m;
                item.Decimal023 = rowIndex * 0.023m;
                item.Decimal024 = rowIndex * 0.024m;
                item.Decimal025 = rowIndex * 0.025m;
                item.Decimal026 = rowIndex * 0.026m;
                item.Decimal027 = rowIndex * 0.027m;
                item.Decimal028 = rowIndex * 0.028m;
                item.Decimal029 = rowIndex * 0.029m;
                item.Decimal030 = rowIndex * 0.030m;
                break;
            case 3:
                item.Decimal031 = rowIndex * 0.031m;
                item.Decimal032 = rowIndex * 0.032m;
                item.Decimal033 = rowIndex * 0.033m;
                item.Decimal034 = rowIndex * 0.034m;
                item.Decimal035 = rowIndex * 0.035m;
                item.Decimal036 = rowIndex * 0.036m;
                item.Decimal037 = rowIndex * 0.037m;
                item.Decimal038 = rowIndex * 0.038m;
                item.Decimal039 = rowIndex * 0.039m;
                item.Decimal040 = rowIndex * 0.040m;
                break;
            case 4:
                item.Decimal041 = rowIndex * 0.041m;
                item.Decimal042 = rowIndex * 0.042m;
                item.Decimal043 = rowIndex * 0.043m;
                item.Decimal044 = rowIndex * 0.044m;
                item.Decimal045 = rowIndex * 0.045m;
                item.Decimal046 = rowIndex * 0.046m;
                item.Decimal047 = rowIndex * 0.047m;
                item.Decimal048 = rowIndex * 0.048m;
                item.Decimal049 = rowIndex * 0.049m;
                item.Decimal050 = rowIndex * 0.050m;
                break;
            case 5:
                item.Decimal051 = rowIndex * 0.051m;
                item.Decimal052 = rowIndex * 0.052m;
                item.Decimal053 = rowIndex * 0.053m;
                item.Decimal054 = rowIndex * 0.054m;
                item.Decimal055 = rowIndex * 0.055m;
                item.Decimal056 = rowIndex * 0.056m;
                item.Decimal057 = rowIndex * 0.057m;
                item.Decimal058 = rowIndex * 0.058m;
                item.Decimal059 = rowIndex * 0.059m;
                item.Decimal060 = rowIndex * 0.060m;
                break;
            case 6:
                item.Decimal061 = rowIndex * 0.061m;
                break;
        }
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
