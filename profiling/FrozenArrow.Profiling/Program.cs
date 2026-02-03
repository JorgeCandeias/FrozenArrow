using FrozenArrow.Profiling.Scenarios;

namespace FrozenArrow.Profiling;

/// <summary>
/// FrozenArrow Profiling Tool
/// 
/// This tool provides detailed performance analysis of ArrowQuery operations.
/// It can be run by both humans and AI to diagnose performance issues.
/// 
/// Usage examples:
///   dotnet run -- --scenario filter --rows 1000000 --iterations 10
///   dotnet run -- --scenario aggregate --rows 500000 --output json
///   dotnet run -- --scenario all --rows 100000 --compare baseline.json
///   dotnet run -- --list
/// </summary>
public static class Program
{
    public static async Task<int> Main(string[] args)
    {
        var config = ParseArguments(args);
        
        if (config.ShowHelp)
        {
            ShowHelp();
            return 0;
        }

        if (config.ListScenarios)
        {
            ListScenarios();
            return 0;
        }

        await RunProfilingAsync(config.Scenario, config);
        return 0;
    }

    private static ProfilingConfig ParseArguments(string[] args)
    {
        var config = new ProfilingConfig();
        string scenario = "all";
        bool showHelp = false;
        bool listScenarios = false;

        for (int i = 0; i < args.Length; i++)
        {
            var arg = args[i].ToLowerInvariant();
            
            switch (arg)
            {
                case "--help" or "-h" or "-?":
                    showHelp = true;
                    break;
                case "--list" or "-l":
                    listScenarios = true;
                    break;
                case "--scenario" or "-s":
                    if (i + 1 < args.Length) scenario = args[++i];
                    break;
                case "--rows" or "-r":
                    if (i + 1 < args.Length && int.TryParse(args[++i], out var rows)) config.RowCount = rows;
                    break;
                case "--iterations" or "-i":
                    if (i + 1 < args.Length && int.TryParse(args[++i], out var iter)) config.Iterations = iter;
                    break;
                case "--warmup" or "-w":
                    if (i + 1 < args.Length && int.TryParse(args[++i], out var warmup)) config.WarmupIterations = warmup;
                    break;
                case "--output" or "-o":
                    if (i + 1 < args.Length && Enum.TryParse<OutputFormat>(args[++i], true, out var format)) config.OutputFormat = format;
                    break;
                case "--save":
                    config.SavePath = i + 1 < args.Length && !args[i + 1].StartsWith("-") ? args[++i] : "";
                    break;
                case "--compare" or "-c":
                    if (i + 1 < args.Length) config.CompareBaseline = args[++i];
                    break;
                case "--verbose" or "-v":
                    config.Verbose = true;
                    break;
                case "--no-parallel":
                    config.EnableParallel = false;
                    break;
            }
        }

        return new ProfilingConfig
        {
            RowCount = config.RowCount,
            Iterations = config.Iterations,
            WarmupIterations = config.WarmupIterations,
            OutputFormat = config.OutputFormat,
            Verbose = config.Verbose,
            EnableParallel = config.EnableParallel,
            SavePath = config.SavePath,
            CompareBaseline = config.CompareBaseline,
            Scenario = scenario,
            ShowHelp = showHelp,
            ListScenarios = listScenarios
        };
    }

    private static void ShowHelp()
    {
        Console.WriteLine("""
            FrozenArrow Query Profiling Tool
            ================================
            
            Usage: dotnet run -- [options]
            
            Options:
              -s, --scenario <name>     Scenario to run (default: all)
              -r, --rows <count>        Number of rows (default: 100000)
              -i, --iterations <count>  Iterations per scenario (default: 5)
              -w, --warmup <count>      Warmup iterations (default: 2)
              -o, --output <format>     Output: table, json, csv, markdown (default: table)
              --save [path]             Save results to file
              -c, --compare <path>      Compare with baseline file
              -v, --verbose             Show detailed phase breakdown
              --no-parallel             Disable parallel execution
              -l, --list                List available scenarios
              -h, --help                Show this help
            
            Examples:
              dotnet run -- -s filter -r 1000000 -i 10
              dotnet run -- -s all -o json --save results.json
              dotnet run -- -s aggregate -v
            """);
    }

    private static void ListScenarios()
    {
        Console.WriteLine("""
            Available Profiling Scenarios:
            ==============================
            
              filter            Filter operations with varying selectivity
              aggregate         Sum, Average, Min, Max aggregations
              groupby           GroupBy with aggregations
              fused             Fused filter+aggregate (single-pass)
              parallel          Parallel vs sequential comparison
              bitmap            SelectionBitmap operations
              predicate         Predicate evaluation (SIMD vs scalar)
              enumeration       Result materialization
              all               Run all scenarios
            
            Example: dotnet run -- -s filter -r 1000000 -i 10 -o json --save
            """);
    }

    private static async Task RunProfilingAsync(string scenario, ProfilingConfig config)
    {
        Console.WriteLine("FrozenArrow Query Profiler v1.0");
        Console.WriteLine("================================");
        Console.WriteLine($"Rows: {config.RowCount:N0}, Iterations: {config.Iterations}, Warmup: {config.WarmupIterations}");
        Console.WriteLine($"Parallel: {config.EnableParallel}, Output: {config.OutputFormat}");
        Console.WriteLine();

        var runner = new ScenarioRunner(config);
        var results = new List<ProfilingResult>();

        var scenarios = scenario.ToLowerInvariant() switch
        {
            "all" => GetAllScenarios(),
            "filter" => [new FilterScenario()],
            "aggregate" => [new AggregateScenario()],
            "groupby" => [new GroupByScenario()],
            "fused" => [new FusedExecutionScenario()],
            "parallel" => [new ParallelComparisonScenario()],
            "bitmap" => [new BitmapOperationsScenario()],
            "predicate" => [new PredicateEvaluationScenario()],
            "enumeration" => [new EnumerationScenario()],
            _ => throw new ArgumentException($"Unknown scenario: {scenario}")
        };

        foreach (var s in scenarios)
        {
            Console.WriteLine($"Running: {s.Name}...");
            var result = await runner.RunAsync(s);
            results.Add(result);
        }

        // Output results
        var formatter = new OutputFormatter(config);
        var output = formatter.Format(results);
        Console.WriteLine(output);

        // Compare with baseline if provided
        if (config.CompareBaseline is not null)
        {
            var comparison = await CompareWithBaselineAsync(results, config.CompareBaseline);
            Console.WriteLine(comparison);
        }

        // Save results if requested
        if (config.SavePath is not null)
        {
            var path = config.SavePath == string.Empty
                ? $"profiling-{DateTime.Now:yyyyMMdd-HHmmss}.json"
                : config.SavePath;
            await SaveResultsAsync(results, path);
            Console.WriteLine($"Results saved to: {path}");
        }
    }

    private static List<IProfilingScenario> GetAllScenarios() =>
    [
        new FilterScenario(),
        new AggregateScenario(),
        new GroupByScenario(),
        new FusedExecutionScenario(),
        new ParallelComparisonScenario(),
        new BitmapOperationsScenario(),
        new PredicateEvaluationScenario(),
        new EnumerationScenario()
    ];

    private static async Task<string> CompareWithBaselineAsync(List<ProfilingResult> current, string baselinePath)
    {
        if (!File.Exists(baselinePath))
        {
            return $"Warning: Baseline file not found: {baselinePath}";
        }

        var baselineJson = await File.ReadAllTextAsync(baselinePath);
        var baseline = System.Text.Json.JsonSerializer.Deserialize<List<ProfilingResult>>(baselineJson);

        if (baseline is null)
        {
            return "Warning: Could not parse baseline file";
        }

        var sb = new System.Text.StringBuilder();
        sb.AppendLine();
        sb.AppendLine("Comparison with Baseline");
        sb.AppendLine("========================");

        foreach (var curr in current)
        {
            var prev = baseline.FirstOrDefault(b => b.ScenarioName == curr.ScenarioName);
            if (prev is null) continue;

            var diff = (curr.MedianMicroseconds - prev.MedianMicroseconds) / prev.MedianMicroseconds * 100;
            var arrow = diff > 0 ? "?" : diff < 0 ? "?" : "?";
            var color = diff > 5 ? "slower" : diff < -5 ? "faster" : "same";

            sb.AppendLine($"  {curr.ScenarioName}: {diff:+0.0;-0.0}% {arrow} ({color})");
        }

        return sb.ToString();
    }

    private static async Task SaveResultsAsync(List<ProfilingResult> results, string path)
    {
        var json = System.Text.Json.JsonSerializer.Serialize(results, new System.Text.Json.JsonSerializerOptions
        {
            WriteIndented = true
        });
        await File.WriteAllTextAsync(path, json);
    }
}
