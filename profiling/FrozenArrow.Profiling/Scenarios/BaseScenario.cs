using System.Diagnostics;
using FrozenArrow.Query;

namespace FrozenArrow.Profiling.Scenarios;

/// <summary>
/// Base class for profiling scenarios with common setup/teardown.
/// </summary>
public abstract class BaseScenario : IProfilingScenario
{
    protected FrozenArrow<ProfilingRecord> Data = null!;
    protected ProfilingConfig Config = null!;
    protected Stopwatch PhaseStopwatch = new();
    protected Dictionary<string, double> CurrentPhases = [];

    public abstract string Name { get; }
    public abstract string Description { get; }

    public virtual void Setup(ProfilingConfig config)
    {
        Config = config;
        var records = ProfilingDataFactory.Generate(config.RowCount);
        Data = records.ToFrozenArrow();
    }

    public abstract object? RunIteration();

    public virtual (object? Result, Dictionary<string, double> PhaseMicroseconds) RunIterationWithPhases()
    {
        CurrentPhases.Clear();
        var result = RunIteration();
        return (result, new Dictionary<string, double>(CurrentPhases));
    }

    protected void StartPhase(string name)
    {
        PhaseStopwatch.Restart();
    }

    protected void EndPhase(string name)
    {
        PhaseStopwatch.Stop();
        if (!CurrentPhases.ContainsKey(name))
            CurrentPhases[name] = 0;
        CurrentPhases[name] += PhaseStopwatch.Elapsed.TotalMicroseconds;
    }

    public virtual void Cleanup()
    {
        Data?.Dispose();
        Data = null!;
    }

    public virtual Dictionary<string, string> GetMetadata() => [];
}
