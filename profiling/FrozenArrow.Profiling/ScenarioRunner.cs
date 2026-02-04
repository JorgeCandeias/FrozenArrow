using System.Diagnostics;
using System.Runtime.InteropServices;

namespace FrozenArrow.Profiling;

/// <summary>
/// Interface for profiling scenarios.
/// </summary>
public interface IProfilingScenario
{
    /// <summary>
    /// Name of the scenario.
    /// </summary>
    string Name { get; }

    /// <summary>
    /// Description of what the scenario measures.
    /// </summary>
    string Description { get; }

    /// <summary>
    /// Setup the scenario with the given row count.
    /// </summary>
    void Setup(ProfilingConfig config);

    /// <summary>
    /// Run a single iteration of the scenario.
    /// Returns the result to prevent dead code elimination.
    /// </summary>
    object? RunIteration();

    /// <summary>
    /// Run iteration with detailed phase tracking.
    /// </summary>
    (object? Result, Dictionary<string, double> PhaseMicroseconds) RunIterationWithPhases();

    /// <summary>
    /// Cleanup after profiling.
    /// </summary>
    void Cleanup();

    /// <summary>
    /// Get additional metadata about the scenario.
    /// </summary>
    Dictionary<string, string> GetMetadata();
}

/// <summary>
/// Runs profiling scenarios and collects results with improved stability.
/// </summary>
/// <remarks>
/// Key improvements for reducing variance:
/// - Process priority elevation during measurement
/// - Outlier detection and removal using IQR method
/// - Stability detection via Coefficient of Variation
/// - Optional GC between iterations
/// - Thread priority elevation
/// </remarks>
public sealed class ScenarioRunner
{
    private readonly ProfilingConfig _config;

    public ScenarioRunner(ProfilingConfig config)
    {
        _config = config;
    }

    public Task<ProfilingResult> RunAsync(IProfilingScenario scenario)
    {
        // Setup
        scenario.Setup(_config);

        // Elevate process priority to reduce interference
        ProcessPriorityClass? originalPriority = null;
        ThreadPriority? originalThreadPriority = null;
        
        if (_config.ElevateProcessPriority)
        {
            try
            {
                var process = Process.GetCurrentProcess();
                originalPriority = process.PriorityClass;
                process.PriorityClass = ProcessPriorityClass.High;
                
                originalThreadPriority = Thread.CurrentThread.Priority;
                Thread.CurrentThread.Priority = ThreadPriority.Highest;
            }
            catch
            {
                // Ignore if we can't elevate priority (e.g., insufficient permissions)
            }
        }

        try
        {
            return Task.FromResult(RunMeasurement(scenario));
        }
        finally
        {
            // Restore original priorities
            if (originalPriority.HasValue)
            {
                try
                {
                    Process.GetCurrentProcess().PriorityClass = originalPriority.Value;
                }
                catch { }
            }
            
            if (originalThreadPriority.HasValue)
            {
                try
                {
                    Thread.CurrentThread.Priority = originalThreadPriority.Value;
                }
                catch { }
            }
        }
    }

    private ProfilingResult RunMeasurement(IProfilingScenario scenario)
    {
        // Force full GC before measurements
        ForceFullGc();

        // Warmup phase - JIT compile and warm caches
        for (int i = 0; i < _config.WarmupIterations; i++)
        {
            _ = scenario.RunIteration();
        }

        // Force GC after warmup to clean up warmup allocations
        ForceFullGc();
        
        // Small delay to let system stabilize
        Thread.Sleep(10);

        var rawSamples = new List<double>(_config.Iterations);
        Dictionary<string, List<double>>? phaseTimings = null;

        if (_config.Verbose)
        {
            phaseTimings = [];
        }

        var sw = new Stopwatch();
        long allocatedBefore = GC.GetAllocatedBytesForCurrentThread();

        for (int i = 0; i < _config.Iterations; i++)
        {
            // Optional GC between iterations for more stable measurements
            if (_config.GcBetweenIterations && i > 0)
            {
                ForceFullGc();
                Thread.Sleep(1); // Brief pause after GC
            }

            if (_config.Verbose)
            {
                var (_, phases) = scenario.RunIterationWithPhases();
                foreach (var (phase, micros) in phases)
                {
                    if (!phaseTimings!.ContainsKey(phase))
                        phaseTimings[phase] = [];
                    phaseTimings[phase].Add(micros);
                }
                // Run again for total timing
                sw.Restart();
                _ = scenario.RunIteration();
                sw.Stop();
            }
            else
            {
                sw.Restart();
                _ = scenario.RunIteration();
                sw.Stop();
            }

            rawSamples.Add(sw.Elapsed.TotalMicroseconds);
        }

        long allocatedAfter = GC.GetAllocatedBytesForCurrentThread();
        long allocatedPerIteration = (allocatedAfter - allocatedBefore) / _config.Iterations;

        // Apply outlier removal if enabled
        var (filteredSamples, outliersRemoved) = _config.RemoveOutliers 
            ? RemoveOutliersIqr(rawSamples, _config.OutlierIqrFactor)
            : (rawSamples.ToArray(), 0);

        // Check stability
        var (isStable, stabilityWarning) = CheckStability(filteredSamples, _config);

        // Build phase details
        Dictionary<string, PhaseTimings>? phaseDetails = null;
        if (phaseTimings is not null)
        {
            var totalAvg = filteredSamples.Average();
            phaseDetails = phaseTimings.ToDictionary(
                kvp => kvp.Key,
                kvp => new PhaseTimings
                {
                    PhaseName = kvp.Key,
                    SamplesMicroseconds = [.. kvp.Value],
                    PercentageOfTotal = kvp.Value.Average() / totalAvg * 100,
                    InvocationCount = kvp.Value.Count / _config.Iterations
                });
        }

        scenario.Cleanup();

        return new ProfilingResult
        {
            ScenarioName = scenario.Name,
            Description = scenario.Description,
            RowCount = _config.RowCount,
            Iterations = _config.Iterations,
            SamplesMicroseconds = filteredSamples,
            PhaseDetails = phaseDetails,
            AllocatedBytes = allocatedPerIteration,
            Metadata = scenario.GetMetadata(),
            OutliersRemoved = outliersRemoved,
            IsStable = isStable,
            StabilityWarning = stabilityWarning
        };
    }

    /// <summary>
    /// Forces a full garbage collection and waits for completion.
    /// </summary>
    private static void ForceFullGc()
    {
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
        GC.WaitForPendingFinalizers();
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
    }

    /// <summary>
    /// Removes outliers using the Interquartile Range (IQR) method.
    /// Values outside [Q1 - factor*IQR, Q3 + factor*IQR] are removed.
    /// </summary>
    private static (double[] FilteredSamples, int OutliersRemoved) RemoveOutliersIqr(
        List<double> samples, 
        double factor)
    {
        if (samples.Count < 4)
        {
            return (samples.ToArray(), 0);
        }

        var sorted = samples.OrderBy(x => x).ToList();
        
        // Calculate Q1 (25th percentile) and Q3 (75th percentile)
        int q1Index = sorted.Count / 4;
        int q3Index = 3 * sorted.Count / 4;
        
        double q1 = sorted[q1Index];
        double q3 = sorted[q3Index];
        double iqr = q3 - q1;
        
        // Define bounds
        double lowerBound = q1 - factor * iqr;
        double upperBound = q3 + factor * iqr;
        
        // Filter outliers
        var filtered = samples.Where(x => x >= lowerBound && x <= upperBound).ToArray();
        int removed = samples.Count - filtered.Length;
        
        // Ensure we keep at least 50% of samples
        if (filtered.Length < samples.Count / 2)
        {
            return (samples.ToArray(), 0);
        }
        
        return (filtered, removed);
    }

    /// <summary>
    /// Checks if measurements are stable using Coefficient of Variation.
    /// </summary>
    private static (bool IsStable, string? Warning) CheckStability(
        double[] samples, 
        ProfilingConfig config)
    {
        if (samples.Length < config.MinValidSamples)
        {
            return (false, $"Too few samples ({samples.Length}) after outlier removal. Increase iterations.");
        }

        var mean = samples.Average();
        if (mean <= 0)
        {
            return (false, "Invalid measurements (mean <= 0)");
        }

        var stdDev = Math.Sqrt(samples.Sum(x => (x - mean) * (x - mean)) / (samples.Length - 1));
        var cv = stdDev / mean;

        if (cv > config.StabilityThreshold)
        {
            var maxMinRatio = samples.Max() / samples.Min();
            return (false, $"High variance detected (CV={cv:P1}, max/min={maxMinRatio:F1}x). " +
                          "Consider: more warmup, closing other apps, or using --gc-between-iterations.");
        }

        return (true, null);
    }
}
