using DuckDB.NET.Data;
using FrozenArrow.MemoryAnalysis.Shared;
using static FrozenArrow.MemoryAnalysis.Shared.AnalysisHelpers;

namespace FrozenArrow.MemoryAnalysis;

/// <summary>
/// Analyzes memory footprint for the standard model (7 columns) across all technologies.
/// Compares List, FrozenArrow, and DuckDB side-by-side.
/// </summary>
public static class StandardModelAnalyzer
{
    public static void Run()
    {
        PrintHeader("STANDARD MODEL MEMORY ANALYSIS (7 columns)");
        
        Console.WriteLine("Comparing List<T>, FrozenArrow<T>, and DuckDB for the standard model.");
        Console.WriteLine("Using Process.PrivateMemorySize64 to capture both managed and native memory.");
        Console.WriteLine();

        WarmUp();

        PrintSubHeader("STATIC MEMORY FOOTPRINT");
        RunStaticMemoryComparison();

        PrintSubHeader("QUERY MEMORY OVERHEAD");
        RunQueryMemoryComparison();
    }

    private static void WarmUp()
    {
        Console.WriteLine("Warming up...");

        // Warm up FrozenArrow
        var warmupList = MemoryAnalysisItemFactory.Generate(1000);
        using var warmupFrozen = warmupList.ToFrozenArrow();
        _ = warmupFrozen.AsQueryable().Where(x => x.Age > 30).Count();

        // Warm up DuckDB
        using var warmupConn = new DuckDBConnection("DataSource=:memory:");
        warmupConn.Open();
        using var cmd = warmupConn.CreateCommand();
        cmd.CommandText = "SELECT 1";
        cmd.ExecuteScalar();

        ForceGC();
        Console.WriteLine("Warmup complete.");
        Console.WriteLine();
    }

    private static void RunStaticMemoryComparison()
    {
        var itemCounts = new[] { 10_000, 100_000, 1_000_000 };

        Console.WriteLine($"{"Items",-12} {"List<T>",-12} {"FrozenArrow",-12} {"DuckDB",-12} {"FA vs List",-15} {"FA vs Duck",-15}");
        Console.WriteLine(new string('-', 78));

        foreach (var count in itemCounts)
        {
            var (listMem, faMem, duckMem) = MeasureStaticMemory(count);

            var faVsListRatio = (double)faMem / Math.Max(listMem, 1);
            var faVsDuckRatio = (double)faMem / Math.Max(duckMem, 1);

            Console.WriteLine($"{count,-12:N0} {FormatBytes(listMem),-12} {FormatBytes(faMem),-12} {FormatBytes(duckMem),-12} {FormatRatio(faVsListRatio),-15} {FormatRatio(faVsDuckRatio),-15}");
        }

        Console.WriteLine();
    }

    private static (long ListMemory, long FrozenArrowMemory, long DuckDbMemory) MeasureStaticMemory(int itemCount)
    {
        long listMemory, frozenArrowMemory, duckDbMemory;

        // Measure List<T>
        {
            ForceGC();
            var baseline = GetProcessMemory();

            var list = MemoryAnalysisItemFactory.Generate(itemCount);
            ForceGC();
            listMemory = GetProcessMemory() - baseline;

            GC.KeepAlive(list);
        }

        ForceGC();
        Thread.Sleep(100);

        // Measure FrozenArrow
        {
            ForceGC();
            var baseline = GetProcessMemory();

            var frozen = MemoryAnalysisItemFactory.GenerateEnumerable(itemCount).ToFrozenArrow();
            ForceGC();
            frozenArrowMemory = GetProcessMemory() - baseline;

            frozen.Dispose();
        }

        ForceGC();
        Thread.Sleep(100);

        // Measure DuckDB
        {
            ForceGC();
            var baseline = GetProcessMemory();

            using var conn = new DuckDBConnection("DataSource=:memory:");
            conn.Open();
            CreateAndPopulateDuckDbTable(conn, itemCount);
            ForceGC();
            duckDbMemory = GetProcessMemory() - baseline;
        }

        ForceGC();
        Thread.Sleep(100);

        return (listMemory, frozenArrowMemory, duckDbMemory);
    }

    private static void RunQueryMemoryComparison()
    {
        const int itemCount = 500_000;

        Console.WriteLine($"Dataset: {itemCount:N0} items");
        Console.WriteLine();
        Console.WriteLine($"{"Query Type",-30} {"List<T>",-12} {"FrozenArrow",-12} {"DuckDB",-12}");
        Console.WriteLine(new string('-', 66));

        // Setup data
        var list = MemoryAnalysisItemFactory.Generate(itemCount);
        using var frozen = list.ToFrozenArrow();

        using var conn = new DuckDBConnection("DataSource=:memory:");
        conn.Open();
        CreateAndPopulateDuckDbTable(conn, list);

        ForceGC();

        // Count query (no materialization)
        var (listCountMem, faCountMem, duckCountMem) = MeasureQueryMemory(
            () => list.Where(x => x.Age > 40).Count(),
            () => frozen.AsQueryable().Where(x => x.Age > 40).Count(),
            () =>
            {
                using var cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT COUNT(*) FROM items WHERE Age > 40";
                return Convert.ToInt32(cmd.ExecuteScalar());
            });

        Console.WriteLine($"{"Count (no materialization)",-30} {FormatBytes(listCountMem),-12} {FormatBytes(faCountMem),-12} {FormatBytes(duckCountMem),-12}");

        // Sum aggregation
        var (listSumMem, faSumMem, duckSumMem) = MeasureQueryMemory(
            () => list.Where(x => x.IsActive).Sum(x => x.Salary),
            () => frozen.AsQueryable().Where(x => x.IsActive).Sum(x => x.Salary),
            () =>
            {
                using var cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT SUM(Salary) FROM items WHERE IsActive = true";
                return Convert.ToDecimal(cmd.ExecuteScalar());
            });

        Console.WriteLine($"{"Sum aggregation",-30} {FormatBytes(listSumMem),-12} {FormatBytes(faSumMem),-12} {FormatBytes(duckSumMem),-12}");

        // GroupBy aggregation
        var (listGroupMem, faGroupMem, duckGroupMem) = MeasureQueryMemory(
            () => list.GroupBy(x => x.Category).ToDictionary(g => g.Key, g => g.Sum(x => x.Salary)),
            () => frozen.AsQueryable().GroupBy(x => x.Category).ToDictionary(g => g.Key, g => g.Sum(x => x.Salary)),
            () =>
            {
                var result = new Dictionary<string, decimal>();
                using var cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT Category, SUM(Salary) FROM items GROUP BY Category";
                using var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    result[reader.GetString(0)] = reader.GetDecimal(1);
                }
                return result;
            });

        Console.WriteLine($"{"GroupBy + Sum",-30} {FormatBytes(listGroupMem),-12} {FormatBytes(faGroupMem),-12} {FormatBytes(duckGroupMem),-12}");

        Console.WriteLine();
    }

    private static (long ListMemory, long FrozenArrowMemory, long DuckDbMemory) MeasureQueryMemory<T>(
        Func<T> listQuery, Func<T> frozenArrowQuery, Func<T> duckDbQuery)
    {
        const int iterations = 5;

        // Measure List
        ForceGC();
        var listBaseline = GetProcessMemory();
        for (int i = 0; i < iterations; i++)
        {
            var result = listQuery();
            GC.KeepAlive(result);
        }
        var listPeak = GetProcessMemory();
        ForceGC();
        var listMemory = Math.Max(0, listPeak - listBaseline);

        // Measure FrozenArrow
        ForceGC();
        var faBaseline = GetProcessMemory();
        for (int i = 0; i < iterations; i++)
        {
            var result = frozenArrowQuery();
            GC.KeepAlive(result);
        }
        var faPeak = GetProcessMemory();
        ForceGC();
        var faMemory = Math.Max(0, faPeak - faBaseline);

        // Measure DuckDB
        ForceGC();
        var duckBaseline = GetProcessMemory();
        for (int i = 0; i < iterations; i++)
        {
            var result = duckDbQuery();
            GC.KeepAlive(result);
        }
        var duckPeak = GetProcessMemory();
        ForceGC();
        var duckMemory = Math.Max(0, duckPeak - duckBaseline);

        return (listMemory, faMemory, duckMemory);
    }

    private static void CreateAndPopulateDuckDbTable(DuckDBConnection conn, int itemCount)
    {
        var items = MemoryAnalysisItemFactory.Generate(itemCount);
        CreateAndPopulateDuckDbTable(conn, items);
    }

    private static void CreateAndPopulateDuckDbTable(DuckDBConnection conn, List<MemoryAnalysisItem> items)
    {
        using var createCmd = conn.CreateCommand();
        createCmd.CommandText = """
            CREATE TABLE items (
                Id INTEGER,
                Name VARCHAR,
                Age INTEGER,
                Salary DECIMAL(18,2),
                IsActive BOOLEAN,
                Category VARCHAR,
                Department VARCHAR
            )
            """;
        createCmd.ExecuteNonQuery();

        using var appender = conn.CreateAppender("items");
        foreach (var item in items)
        {
            var row = appender.CreateRow();
            row.AppendValue(item.Id);
            row.AppendValue(item.Name);
            row.AppendValue(item.Age);
            row.AppendValue(item.Salary);
            row.AppendValue(item.IsActive);
            row.AppendValue(item.Category);
            row.AppendValue(item.Department);
            row.EndRow();
        }
    }
}
