using BenchmarkDotNet.Running;
using ArrowCollection.Benchmarks;

// Use BenchmarkSwitcher for flexible benchmark selection
// Run with --help to see all options, or --list to see available benchmarks
// Examples:
//   dotnet run -c Release                              # Interactive selection
//   dotnet run -c Release -- --filter "*Query*"        # Run all query benchmarks
//   dotnet run -c Release -- --filter "*HighSelectivity*"  # Run specific category
//   dotnet run -c Release -- --list flat               # List all benchmarks

var switcher = BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly);

if (args.Length == 0)
{
    Console.WriteLine("ArrowCollection Benchmark Runner");
    Console.WriteLine("================================");
    Console.WriteLine();
    Console.WriteLine("Available benchmark classes:");
    Console.WriteLine("  - ArrowCollectionBenchmarks     : Core construction/enumeration benchmarks");
    Console.WriteLine("  - StructVsClassBenchmarks       : Struct vs class comparison");
    Console.WriteLine("  - HeavyRecordBenchmarks         : 200-property record benchmarks");
    Console.WriteLine("  - ArrowQueryBenchmarks          : ArrowQuery vs List vs Enumerable (10 columns)");
    Console.WriteLine("  - WideRecordQueryBenchmarks     : ArrowQuery benchmarks for wide records (200 columns)");
    Console.WriteLine();
    Console.WriteLine("Usage examples:");
    Console.WriteLine("  dotnet run -c Release                                    # Interactive selection");
    Console.WriteLine("  dotnet run -c Release -- --filter *Query*                # Run all query benchmarks");
    Console.WriteLine("  dotnet run -c Release -- --filter *HighSelectivity*      # Run specific category");
    Console.WriteLine("  dotnet run -c Release -- --filter ArrowQueryBenchmarks*  # Run specific class");
    Console.WriteLine("  dotnet run -c Release -- --list flat                     # List all benchmarks");
    Console.WriteLine("  dotnet run -c Release -- --help                          # Show BenchmarkDotNet help");
    Console.WriteLine();
    Console.WriteLine("For memory footprint analysis, run the ArrowCollection.MemoryAnalysis project.");
    Console.WriteLine();
}

switcher.Run(args);


