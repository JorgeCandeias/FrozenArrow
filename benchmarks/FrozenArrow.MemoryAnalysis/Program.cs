using FrozenArrow.MemoryAnalysis;

Console.WriteLine("FrozenArrow Memory Footprint Analysis");
Console.WriteLine("=====================================");
Console.WriteLine();
Console.WriteLine("This analysis compares memory footprint across technologies:");
Console.WriteLine("  - List<T>     : Standard .NET collection");
Console.WriteLine("  - FrozenArrow : Columnar Arrow-backed collection");
Console.WriteLine("  - DuckDB      : In-process analytical database");
Console.WriteLine();
Console.WriteLine("Analysis is organized by data model:");
Console.WriteLine("  1. Standard Model (7 columns)  - All technologies side-by-side");
Console.WriteLine("  2. Wide Model (200 columns)    - List vs FrozenArrow");
Console.WriteLine();

// Run standard model analysis (all technologies)
StandardModelAnalyzer.Run();

// Run wide model analysis (List vs FrozenArrow)
// Note: DuckDB not included due to complexity of 200-column table setup
HeavyRecordMemoryAnalyzer.Run();

