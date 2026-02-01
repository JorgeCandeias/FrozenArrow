using BenchmarkDotNet.Running;
using ArrowCollection.Benchmarks;

// Check for command-line arguments
if (args.Length > 0 && args[0] == "--struct-comparison")
{
    // Run struct vs class comparison benchmarks
    BenchmarkRunner.Run<StructVsClassBenchmarks>();
    return;
}

if (args.Length > 0 && args[0] == "--heavy")
{
    // Run extreme scenario benchmarks with 200-property record and 1M items
    BenchmarkRunner.Run<HeavyRecordBenchmarks>();
    return;
}

if (args.Length > 0 && args[0] == "--query")
{
    // Run ArrowQuery benchmarks
    BenchmarkRunner.Run<ArrowQueryBenchmarks>();
    return;
}

if (args.Length > 0 && args[0] == "--query-wide")
{
    // Run ArrowQuery benchmarks for wide records
    BenchmarkRunner.Run<WideRecordQueryBenchmarks>();
    return;
}

if (args.Length > 0 && args[0] == "--all")
{
    // Run all BenchmarkDotNet benchmarks
    BenchmarkRunner.Run<ArrowCollectionBenchmarks>();
    BenchmarkRunner.Run<StructVsClassBenchmarks>();
    BenchmarkRunner.Run<HeavyRecordBenchmarks>();
    BenchmarkRunner.Run<ArrowQueryBenchmarks>();
    BenchmarkRunner.Run<WideRecordQueryBenchmarks>();
    return;
}

// Default: run the main benchmarks
Console.WriteLine("ArrowCollection Benchmark Runner");
Console.WriteLine("================================");
Console.WriteLine();
Console.WriteLine("Available options:");
Console.WriteLine("  --struct-comparison  : Run struct vs class comparison benchmarks");
Console.WriteLine("  --heavy              : Run extreme scenario (200-property record, 1M items)");
Console.WriteLine("  --query              : Run ArrowQuery vs List vs Enumerable benchmarks");
Console.WriteLine("  --query-wide         : Run ArrowQuery benchmarks for wide records (200 columns)");
Console.WriteLine("  --all                : Run all BenchmarkDotNet benchmarks");
Console.WriteLine("  (no args)            : Run main ArrowCollection benchmarks");
Console.WriteLine();
Console.WriteLine("For memory footprint analysis, run the ArrowCollection.MemoryAnalysis project.");
Console.WriteLine();

BenchmarkRunner.Run<ArrowCollectionBenchmarks>();


