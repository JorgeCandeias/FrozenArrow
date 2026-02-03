using System.Diagnostics;

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
/// Runs profiling scenarios and collects results.
/// </summary>
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

        // Force GC before measurements
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        // Warmup
        for (int i = 0; i < _config.WarmupIterations; i++)
        {
            _ = scenario.RunIteration();
        }

        // Force GC after warmup
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        var samples = new double[_config.Iterations];
        Dictionary<string, List<double>>? phaseTimings = null;

        if (_config.Verbose)
        {
            phaseTimings = [];
        }

        var sw = new Stopwatch();
        long allocatedBefore = GC.GetAllocatedBytesForCurrentThread();

        for (int i = 0; i < _config.Iterations; i++)
        {
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

            samples[i] = sw.Elapsed.TotalMicroseconds;
        }

        long allocatedAfter = GC.GetAllocatedBytesForCurrentThread();
        long allocatedPerIteration = (allocatedAfter - allocatedBefore) / _config.Iterations;

        // Build phase details
        Dictionary<string, PhaseTimings>? phaseDetails = null;
        if (phaseTimings is not null)
        {
            var totalAvg = samples.Average();
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

        var result = new ProfilingResult
        {
            ScenarioName = scenario.Name,
            Description = scenario.Description,
            RowCount = _config.RowCount,
            Iterations = _config.Iterations,
            SamplesMicroseconds = samples,
            PhaseDetails = phaseDetails,
            AllocatedBytes = allocatedPerIteration,
            Metadata = scenario.GetMetadata()
        };

        return Task.FromResult(result);
    }
}
