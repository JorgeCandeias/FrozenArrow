using FrozenArrow.Query;

namespace FrozenArrow.Profiling.Scenarios;

/// <summary>
/// Profiles filter operations with varying selectivity.
/// </summary>
public sealed class FilterScenario : BaseScenario
{
    public override string Name => "Filter";
    public override string Description => "Filter + Count with varying selectivity";

    private int _highSelectivityResult;
    private int _lowSelectivityResult;
    private int _multiFilterResult;

    public override object? RunIteration()
    {
        var query = Data.AsQueryable();
        if (!Config.EnableParallel)
        {
            ((ArrowQueryProvider)query.Provider).ParallelOptions = new ParallelQueryOptions
            {
                EnableParallelExecution = false
            };
        }

        // High selectivity (~10% pass): Age > 55
        _highSelectivityResult = query.Where(x => x.Age > 55).Count();

        // Low selectivity (~70% pass): IsActive
        _lowSelectivityResult = query.Where(x => x.IsActive).Count();

        // Multi-filter: Age > 30 && IsActive && Salary > 50000
        _multiFilterResult = query.Where(x => x.Age > 30 && x.IsActive && x.Salary > 50000).Count();

        return _highSelectivityResult + _lowSelectivityResult + _multiFilterResult;
    }

    public override (object? Result, Dictionary<string, double> PhaseMicroseconds) RunIterationWithPhases()
    {
        CurrentPhases.Clear();
        var query = Data.AsQueryable();
        if (!Config.EnableParallel)
        {
            ((ArrowQueryProvider)query.Provider).ParallelOptions = new ParallelQueryOptions
            {
                EnableParallelExecution = false
            };
        }

        StartPhase("HighSelectivity");
        _highSelectivityResult = query.Where(x => x.Age > 55).Count();
        EndPhase("HighSelectivity");

        StartPhase("LowSelectivity");
        _lowSelectivityResult = query.Where(x => x.IsActive).Count();
        EndPhase("LowSelectivity");

        StartPhase("MultiFilter");
        _multiFilterResult = query.Where(x => x.Age > 30 && x.IsActive && x.Salary > 50000).Count();
        EndPhase("MultiFilter");

        return (_highSelectivityResult + _lowSelectivityResult + _multiFilterResult, new Dictionary<string, double>(CurrentPhases));
    }

    public override Dictionary<string, string> GetMetadata() => new()
    {
        ["HighSelectivity"] = $"Age > 55 -> {_highSelectivityResult} rows",
        ["LowSelectivity"] = $"IsActive -> {_lowSelectivityResult} rows",
        ["MultiFilter"] = $"Age > 30 && IsActive && Salary > 50000 -> {_multiFilterResult} rows"
    };
}
