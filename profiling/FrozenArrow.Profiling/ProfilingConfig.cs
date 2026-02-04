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
    
    /// <summary>
    /// Enable outlier removal using IQR method.
    /// Default: true - removes extreme outliers caused by GC/OS interruptions.
    /// </summary>
    public bool RemoveOutliers { get; set; } = true;
    
    /// <summary>
    /// Outlier removal factor (multiplier for IQR).
    /// Default: 1.5 (standard IQR method for mild outliers).
    /// Use 3.0 for extreme outliers only.
    /// </summary>
    public double OutlierIqrFactor { get; set; } = 1.5;
    
    /// <summary>
    /// Force GC between each measured iteration to reduce variance.
    /// Default: false (adds overhead but reduces variance).
    /// </summary>
    public bool GcBetweenIterations { get; set; } = false;
    
    /// <summary>
    /// Elevate process priority during measurement.
    /// Default: true - reduces interference from other processes.
    /// Requires appropriate permissions on some systems.
    /// </summary>
    public bool ElevateProcessPriority { get; set; } = true;
    
    /// <summary>
    /// Coefficient of Variation threshold for stability warning.
    /// If CV exceeds this, results are flagged as unstable.
    /// Default: 0.15 (15% - higher values tolerate more variance).
    /// </summary>
    public double StabilityThreshold { get; set; } = 0.15;
    
    /// <summary>
    /// Minimum number of valid samples required after outlier removal.
    /// Default: 3 - if fewer samples remain, results are flagged.
    /// </summary>
    public int MinValidSamples { get; set; } = 3;
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
