using System.Text.Json.Serialization;

namespace FrozenArrow.Profiling;

/// <summary>
/// Results from a profiling scenario.
/// </summary>
public sealed class ProfilingResult
{
    /// <summary>
    /// Name of the scenario.
    /// </summary>
    public required string ScenarioName { get; init; }

    /// <summary>
    /// Description of what was measured.
    /// </summary>
    public required string Description { get; init; }

    /// <summary>
    /// Number of rows processed.
    /// </summary>
    public int RowCount { get; init; }

    /// <summary>
    /// Number of iterations.
    /// </summary>
    public int Iterations { get; init; }

    /// <summary>
    /// All timing samples in microseconds.
    /// </summary>
    public required double[] SamplesMicroseconds { get; init; }

    /// <summary>
    /// Minimum time in microseconds.
    /// </summary>
    public double MinMicroseconds => SamplesMicroseconds.Length > 0 ? SamplesMicroseconds.Min() : 0;

    /// <summary>
    /// Maximum time in microseconds.
    /// </summary>
    public double MaxMicroseconds => SamplesMicroseconds.Length > 0 ? SamplesMicroseconds.Max() : 0;

    /// <summary>
    /// Median time in microseconds.
    /// </summary>
    public double MedianMicroseconds
    {
        get
        {
            if (SamplesMicroseconds.Length == 0) return 0;
            var sorted = SamplesMicroseconds.OrderBy(x => x).ToArray();
            var mid = sorted.Length / 2;
            return sorted.Length % 2 == 0
                ? (sorted[mid - 1] + sorted[mid]) / 2
                : sorted[mid];
        }
    }

    /// <summary>
    /// Average time in microseconds.
    /// </summary>
    public double AverageMicroseconds => SamplesMicroseconds.Length > 0 ? SamplesMicroseconds.Average() : 0;

    /// <summary>
    /// Standard deviation in microseconds.
    /// </summary>
    public double StdDevMicroseconds
    {
        get
        {
            if (SamplesMicroseconds.Length < 2) return 0;
            var avg = AverageMicroseconds;
            var sumSquares = SamplesMicroseconds.Sum(x => (x - avg) * (x - avg));
            return Math.Sqrt(sumSquares / (SamplesMicroseconds.Length - 1));
        }
    }

    /// <summary>
    /// Throughput in rows per second.
    /// </summary>
    public double RowsPerSecond => MedianMicroseconds > 0 ? RowCount / (MedianMicroseconds / 1_000_000.0) : 0;

    /// <summary>
    /// Throughput in millions of rows per second.
    /// </summary>
    public double MillionRowsPerSecond => RowsPerSecond / 1_000_000.0;

    /// <summary>
    /// Detailed phase timings (only populated in verbose mode).
    /// </summary>
    public Dictionary<string, PhaseTimings>? PhaseDetails { get; init; }

    /// <summary>
    /// Memory allocation in bytes (if measured).
    /// </summary>
    public long? AllocatedBytes { get; init; }

    /// <summary>
    /// Additional metadata about the scenario.
    /// </summary>
    public Dictionary<string, string>? Metadata { get; init; }

    /// <summary>
    /// Timestamp when the profiling was run.
    /// </summary>
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;
    
    /// <summary>
    /// Number of outliers removed from the samples.
    /// </summary>
    public int OutliersRemoved { get; init; }
    
    /// <summary>
    /// Coefficient of Variation (StdDev / Mean).
    /// Lower values indicate more stable measurements.
    /// </summary>
    [JsonIgnore]
    public double CoefficientOfVariation => AverageMicroseconds > 0 
        ? StdDevMicroseconds / AverageMicroseconds 
        : 0;
    
    /// <summary>
    /// Whether the measurements are considered stable (CV below threshold).
    /// </summary>
    public bool IsStable { get; init; } = true;
    
    /// <summary>
    /// Warning message if measurements are unstable.
    /// </summary>
    public string? StabilityWarning { get; init; }
    
    /// <summary>
    /// Trimmed mean (average after removing outliers).
    /// More robust than regular mean.
    /// </summary>
    [JsonIgnore]
    public double TrimmedMeanMicroseconds
    {
        get
        {
            if (SamplesMicroseconds.Length < 4) return AverageMicroseconds;
            var sorted = SamplesMicroseconds.OrderBy(x => x).ToArray();
            // Remove top and bottom 10%
            var trimCount = Math.Max(1, sorted.Length / 10);
            return sorted.Skip(trimCount).Take(sorted.Length - 2 * trimCount).Average();
        }
    }
    
    /// <summary>
    /// Interquartile range (Q3 - Q1) - measure of statistical dispersion.
    /// </summary>
    [JsonIgnore]
    public double InterquartileRange
    {
        get
        {
            if (SamplesMicroseconds.Length < 4) return 0;
            var sorted = SamplesMicroseconds.OrderBy(x => x).ToArray();
            var q1Index = sorted.Length / 4;
            var q3Index = 3 * sorted.Length / 4;
            return sorted[q3Index] - sorted[q1Index];
        }
    }
}

/// <summary>
/// Timing breakdown for a specific phase of query execution.
/// </summary>
public sealed class PhaseTimings
{
    /// <summary>
    /// Name of the phase.
    /// </summary>
    public required string PhaseName { get; init; }

    /// <summary>
    /// All timing samples in microseconds.
    /// </summary>
    public required double[] SamplesMicroseconds { get; init; }

    /// <summary>
    /// Average time in microseconds.
    /// </summary>
    public double AverageMicroseconds => SamplesMicroseconds.Length > 0 ? SamplesMicroseconds.Average() : 0;

    /// <summary>
    /// Percentage of total time spent in this phase.
    /// </summary>
    public double PercentageOfTotal { get; init; }

    /// <summary>
    /// Number of times this phase was invoked.
    /// </summary>
    public int InvocationCount { get; init; }
}
