using ArrowCollection.MemoryAnalysis;

// Check for command-line arguments
if (args.Length > 0 && args[0] == "--heavy")
{
    // Run extreme scenario: 200-property record with 1M items
    HeavyRecordMemoryAnalyzer.Run();
    return;
}

if (args.Length > 0 && args[0] == "--all")
{
    // Run all memory analysis
    MemoryFootprintAnalyzer.Run();
    HeavyRecordMemoryAnalyzer.Run();
    return;
}

// Default: run standard analysis
Console.WriteLine("ArrowCollection Memory Footprint Analyzer");
Console.WriteLine("=========================================");
Console.WriteLine();
Console.WriteLine("Available options:");
Console.WriteLine("  --heavy  : Run extreme scenario (200-property record, 1M items)");
Console.WriteLine("  --all    : Run all memory analysis");
Console.WriteLine("  (no args): Run standard memory footprint analysis");
Console.WriteLine();

MemoryFootprintAnalyzer.Run();

