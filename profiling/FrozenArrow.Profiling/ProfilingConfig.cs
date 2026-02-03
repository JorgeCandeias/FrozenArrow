namespace FrozenArrow.Profiling;

/// <summary>
/// Configuration for profiling runs.
/// </summary>
public sealed class ProfilingConfig
{
    /// <summary>
    /// Number of rows in the test dataset.
    /// </summary>
    public int RowCount { get; set; } = 100_000;

    /// <summary>
    /// Number of measured iterations per scenario.
    /// </summary>
    public int Iterations { get; set; } = 5;

    /// <summary>
    /// Number of warmup iterations (not measured).
    /// </summary>
    public int WarmupIterations { get; set; } = 2;

    /// <summary>
    /// Output format for results.
    /// </summary>
    public OutputFormat OutputFormat { get; set; } = OutputFormat.Table;

    /// <summary>
    /// Show detailed per-phase timing breakdown.
    /// </summary>
    public bool Verbose { get; set; }

    /// <summary>
    /// Enable parallel query execution.
    /// </summary>
    public bool EnableParallel { get; set; } = true;

    /// <summary>
    /// Path to save results (null = don't save, empty = auto-generate).
    /// </summary>
    public string? SavePath { get; set; }

    /// <summary>
    /// Path to baseline results for comparison.
    /// </summary>
    public string? CompareBaseline { get; set; }

    /// <summary>
    /// Scenario to run.
    /// </summary>
    public string Scenario { get; set; } = "all";

    /// <summary>
    /// Show help and exit.
    /// </summary>
    public bool ShowHelp { get; set; }

    /// <summary>
    /// List scenarios and exit.
    /// </summary>
    public bool ListScenarios { get; set; }
}

/// <summary>
/// Output format options.
/// </summary>
public enum OutputFormat
{
    Table,
    Json,
    Csv,
    Markdown
}
